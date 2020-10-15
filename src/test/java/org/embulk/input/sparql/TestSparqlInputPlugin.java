package org.embulk.input.sparql;

import org.apache.jena.fuseki.main.FusekiServer;
import org.apache.jena.graph.Factory;
import org.apache.jena.graph.Graph;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.riot.RDFParser;
import org.embulk.config.ConfigSource;
import org.embulk.spi.InputPlugin;
import org.embulk.test.EmbulkTests;
import org.embulk.test.TestingEmbulk;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.URL;
import java.nio.file.Path;
import java.util.logging.Logger;

import static org.embulk.test.EmbulkTests.readSortedFile;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

public class TestSparqlInputPlugin
{
    private static final Logger logger = Logger.getLogger(TestSparqlInputPlugin.class.getName());

    private static final String BASIC_RESOURCE_PATH = "org/embulk/input/sparql/test/expect/";

    private static ConfigSource loadYamlResource(TestingEmbulk embulk, String fileName)
    {
        return embulk.loadYamlResource(BASIC_RESOURCE_PATH + fileName);
    }

    private static String readResource(String fileName)
    {
        return EmbulkTests.readResource(BASIC_RESOURCE_PATH + fileName);
    }

    @Rule
    public TestingEmbulk embulk = TestingEmbulk.builder()
            .registerPlugin(InputPlugin.class, "sparql", SparqlInputPlugin.class)
            .build();

    private static FusekiServer fusekiServer = null;

    @BeforeClass
    public static void setup()
    {
        // detect available port
        int availablePort = -1;
        for (int i = 1025; i < 65535; i++) {
            try (ServerSocket socket = new ServerSocket(i, 1, InetAddress.getByName("localhost"))) {
                availablePort = socket.getLocalPort();
                break;
            }
            catch (IOException e) {
                // skip, try next port number
            }
        }
        if (availablePort == -1) {
            fail("cannot find available port.");
        }

        // start fuseki server
        URL rootUrl = TestSparqlInputPlugin.class.getClassLoader().getResource(BASIC_RESOURCE_PATH + "unittest.ttl");
        assertNotNull("testValidatorsImpl not found", rootUrl);
        String ttlPath = rootUrl.getPath();

        Graph g = Factory.createGraphMem();
        RDFParser.source(ttlPath).parse(g);
        Dataset ds = DatasetFactory.create(ModelFactory.createModelForGraph(g));
        fusekiServer = FusekiServer.create()
                .add("/dataset", ds)
                .port(availablePort)
                .build();
        fusekiServer.start();
        logger.info("start fuseki server for unittest.");
    }

    @AfterClass
    public static void tearDown()
    {
        // stop fuseki server
        fusekiServer.stop();
        logger.info("stop fuseki server for unittest.");
    }

    private ConfigSource baseConfig;

    @Before
    public void init()
    {
        // init config
        baseConfig = embulk.newConfig();
        baseConfig.set("type", "sparql");
        baseConfig.set("endpoint", String.format("http://localhost:%d/dataset", fusekiServer.getPort()));
    }

    @Test
    public void testBasic()
            throws Exception
    {
        Path out1 = embulk.createTempFile("csv");
        embulk.runInput(baseConfig.merge(loadYamlResource(embulk, "test_config.yml")), out1);
        assertThat(readSortedFile(out1), is(readResource("test_expected.csv")));
    }

    @Test
    public void testAttrs()
            throws Exception
    {
        Path out1 = embulk.createTempFile("csv");
        embulk.runInput(baseConfig.merge(loadYamlResource(embulk, "test_config_attrs.yml")), out1);
        assertThat(readSortedFile(out1), is(readResource("test_expected_attrs.csv")));
    }

    @Test(expected = org.embulk.exec.PartialExecutionException.class)
    public void testInvalidCols()
            throws Exception
    {
        Path out1 = embulk.createTempFile("csv");
        embulk.runInput(baseConfig.merge(loadYamlResource(embulk, "test_config_invalid_cols.yml")), out1);
    }
}
