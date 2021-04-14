package org.rascalmpl;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.StringWriter;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.regex.Pattern;
import com.google.gson.stream.JsonWriter;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.analysis.miscellaneous.PerFieldAnalyzerWrapper;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.queryparser.classic.MultiFieldQueryParser;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import fi.iki.elonen.NanoHTTPD;
import fi.iki.elonen.NanoHTTPD.Response.Status;

public class Server extends NanoHTTPD {

    private final Path serverRoot;
    private volatile ClosingWrapper<IndexSearcher> stableSearcher;
    private volatile ClosingWrapper<IndexSearcher> unstableSearcher;
    private volatile String currentVersion;
    private final Analyzer fieldAnalyzer;

    public Server(Path serverRoot) {
        super("localhost", 9999);
        this.serverRoot = serverRoot;
        currentVersion = readCurrentVersion();
        installFileWatch();
        buildSearcher();
        fieldAnalyzer = initFieldAnalyzer();
    }

    // reusable, as Standard and Whitespace analyzers do not keep local state
    // has to be kept in sync with rascal!
    private static Analyzer initFieldAnalyzer() {
        Analyzer stdAnalyzer = new StandardAnalyzer();

        HashMap<String,Analyzer> analyzerMap = new HashMap<>();

        //analyzerMap.put("name", new SimpleAnalyzer());
        analyzerMap.put("index", new WhitespaceAnalyzer());
        analyzerMap.put("synopsis", stdAnalyzer);
        analyzerMap.put("signature", stdAnalyzer);
        analyzerMap.put("doc", stdAnalyzer);

        return new PerFieldAnalyzerWrapper(stdAnalyzer, analyzerMap);
    }

    // we have to make sure to close th directory & reader after nobody uses it anymore (overwritten by update from nexus)
    private static class ClosingWrapper<T> {
        private final T reference;
        private final DirectoryReader reader;

        public ClosingWrapper(T reference, DirectoryReader reader) {
            this.reference = reference;
            this.reader = reader;
        }

        public T get() {
            return reference;
        }

        @Override
        protected void finalize() throws Throwable {
            Directory toClose2 = reader.directory();
            try {
                reader.close();
            } finally {
                toClose2.close();
            }
        }

        @Override
        public String toString() {
            return reference.toString();
        }
    }

    private DirectoryReader buildIndex(Path p) throws IOException {
        MMapDirectory dir = new MMapDirectory(p);
        dir.setUseUnmap(true); // try do a accurate close that releases files
        return DirectoryReader.open(dir);
    }

    private void buildSearcher() {
        Path searchDir = serverRoot.resolve("site-" + currentVersion).resolve("search");
        try {
            stableSearcher = buildSearcher(buildIndex(searchDir.resolve("stable")));
            unstableSearcher = buildSearcher(buildIndex(searchDir.resolve("unstable")));
            System.out.println("New indexes loaded");
        } catch (IOException e) {
            synchronized(System.err) {
                System.err.println("Error loading the searchers:" + e.getMessage());
                e.printStackTrace(System.err);
            }
        }
    }

    private ClosingWrapper<IndexSearcher> buildSearcher(DirectoryReader index) {
        return new ClosingWrapper<>(new IndexSearcher(index), index);
    }

    private void installFileWatch() {
        try {
            WatchService watcher = serverRoot.getFileSystem().newWatchService();
            serverRoot.register(watcher, StandardWatchEventKinds.ENTRY_MODIFY);
            Thread t = new Thread(() -> {
                WatchKey key;
                try {
                    while ((key = watcher.take()) != null) {
                        boolean shouldCheck = false;
                        for (WatchEvent<?> event : key.pollEvents()) {
                            if (event.kind() == StandardWatchEventKinds.ENTRY_MODIFY) {
                                shouldCheck = ((Path) event.context()).toString().equals("version");
                            } else if (event.kind() == StandardWatchEventKinds.OVERFLOW) {
                                // check version just to be sure
                                shouldCheck = true;
                            }

                            if (shouldCheck) {
                                break;
                            }
                        }
                        if (shouldCheck) {
                            String newVersion = readCurrentVersion();
                            if (!currentVersion.equals(newVersion)) {
                                currentVersion = newVersion;
                                buildSearcher();
                            }
                        }
                        key.reset(); // signal that we want more events
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }

            });
            t.setName("Update watcher");
            t.setDaemon(true);
            t.start();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private String readCurrentVersion() {
        try {
            return new String(Files.readAllBytes(serverRoot.resolve("version")), StandardCharsets.UTF_8).trim();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Response serve(IHTTPSession session) {
        try {
            if (session.getMethod() == Method.GET && session.getUri().endsWith("/search")) {
                List<String> query = session.getParameters().get("searchFor");
                if (query != null && query.size() == 1) {
                    return search(session.getUri(), URLDecoder.decode(query.get(0), "UTF-8"));
                } else {
                    return newFixedLengthResponse(Status.BAD_REQUEST, MIME_PLAINTEXT, "Missing searchFor parameter");
                }
            }
            return newFixedLengthResponse(Status.BAD_REQUEST, MIME_PLAINTEXT, "Invalid operation");
        } catch (Throwable t) {
            synchronized(System.err) {
                System.err.println("Unexpected error in processing request: " + session.getUri());
                System.err.println(t.getMessage());
                t.printStackTrace(System.err);
            }
            return newFixedLengthResponse(Status.INTERNAL_ERROR, MIME_PLAINTEXT, "Unexpected server error");
        }
    }

    private Response search(String uri, String query) {
        boolean unstable = uri.startsWith("/unstable");
        return search(unstable, unstable ? unstableSearcher : stableSearcher, query);
    }

    private Response search(boolean unstable, ClosingWrapper<IndexSearcher> searcher, String query) {
        try {
            Query parsedQuery = buildQueryParser().parse(escapeForQuery(query));
            if (parsedQuery != null) {
                TopDocs results = searcher.get().search(parsedQuery, 25);
                if (results != null ) {
                    return translateResult(unstable, results.scoreDocs, searcher.get());
                }
                else {
                    return translateResult(unstable, new ScoreDoc[0], searcher.get());
                }
            }
            else {
                return newFixedLengthResponse(Status.INTERNAL_ERROR, MIME_PLAINTEXT, "Failed to parse query: " + query);
            }
        } catch (IOException | ParseException e) {
            return newFixedLengthResponse(Status.INTERNAL_ERROR, MIME_PLAINTEXT, "Error searching: " + e.getMessage());
        }
    }

    private static final String[] fields = new String[] {"index", "synopsis", "doc"};
    private static final Map<String, Float> boosts;
    static {
        boosts = new HashMap<>();
        boosts.put("index", 2f);
        boosts.put("synopsis", 2f);
    }


    private MultiFieldQueryParser buildQueryParser() {
        return new MultiFieldQueryParser(fields, this.fieldAnalyzer, boosts);
    }

    private static Response translateResult(boolean unstable, ScoreDoc[] results, IndexSearcher searcher) {
        ByteArrayOutputStream target = new ByteArrayOutputStream();
        try (JsonWriter w = new JsonWriter(new OutputStreamWriter(target, StandardCharsets.UTF_8))) {
            w.beginObject();
            w.name("results");
            w.beginArray();
            for (ScoreDoc r : results) {
                appendJsonResult(unstable, searcher.doc(r.doc), w);
            }
            w.endArray();
            w.endObject();
        } catch (IOException e) {
        }
        byte[] data = target.toByteArray();
        return newFixedLengthResponse(Status.OK, "application/json; charset=utf-8", new ByteArrayInputStream(data), data.length);
    }

    private static void appendJsonResult(boolean unstable, Document hitDoc, JsonWriter w) throws IOException {
        if (hitDoc != null) {
            w.beginObject();
            String name = hitDoc.get("name");
            w.name("name");
            w.value(name);
            w.name("url");
            w.value(makeURL(unstable, name));
            w.name("text");
            w.value(getField(hitDoc, "synopsis"));
            String signature = getField(hitDoc, "signature");
            if (!signature.isEmpty()) {
                w.name("code");
                w.value(signature);
            }
            w.endObject();
        }
    }

    private static String makeURL(boolean unstable, String conceptName) {
        StringWriter w = new StringWriter();
         w.append(unstable ? "/unstable" : "/stable");
        appendURL(w, conceptName);
        return w.toString();
    }

    private static void appendURL(StringWriter w, String conceptName) {
        String[] parts = conceptName.split("/");
        int n = parts.length;
        String course = parts[0];
        w.append("/")
            .append(course)
            .append("#")
            .append(parts[n - (n > 1 ? 2 : 1)])
            .append("-")
            .append(parts[n - 1]);
    }

    private static String getField(Document hitDoc, String field) {
        String s = hitDoc.get(field);
        return s == null ? "" : s;
    }

    private static final Pattern BAD_QUERY_CHARS =
            Pattern.compile("([" + Pattern.quote("+\\-!(){}[]^\"~*?:/") + "]|(&&)|(\\|\\|))");

    private static String escapeForQuery(String s) {
        return BAD_QUERY_CHARS.matcher(s.toLowerCase()).replaceAll("\\\\$1");
    }

    private static class BoundRunner implements AsyncRunner {
        private final ExecutorService executorService;
        private final Queue<ClientHandler> running = new ConcurrentLinkedQueue<>();

        public BoundRunner(ExecutorService executorService) {
            this.executorService = executorService;
        }

        @Override
        public void closeAll() {
            // copy of the list for concurrency
            for (NanoHTTPD.ClientHandler clientHandler : this.running) {
                clientHandler.close();
            }
        }

        @Override
        public void closed(ClientHandler clientHandler) {
            this.running.remove(clientHandler);
        }

        @Override
        public void exec(ClientHandler clientHandler) {
            executorService.submit(clientHandler);
            this.running.add(clientHandler);
        }
    }


    public static void main(String[] args) throws IOException, InterruptedException {
        Server server = new Server(new File(args[0]).toPath());
        server.setAsyncRunner(new BoundRunner(Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() / 2)));
        server.start(1000, false);
        System.out.println("Server is running, hit ctrl-c to stop");
        while (server.isAlive()) {
            Thread.sleep(1000);
        }
    }
}
