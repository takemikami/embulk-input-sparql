package org.embulk.input.sparql;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.logging.Logger;

import org.apache.jena.query.*;

import org.apache.jena.rdf.model.Literal;
import org.embulk.config.Config;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigSource;
import org.embulk.config.Task;
import org.embulk.config.TaskReport;
import org.embulk.config.TaskSource;
import org.embulk.spi.*;
import org.embulk.spi.type.Type;
import org.embulk.spi.type.Types;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

public class SparqlInputPlugin
        implements InputPlugin
{
    private static final Logger logger = Logger.getLogger(SparqlInputPlugin.class.getName());

    public interface PluginTask
            extends Task
    {
        // endpoint: sparql endpoint (required)
        @Config("endpoint")
        public String getEndpoint();

        // query: sparql query (required)
        @Config("query")
        public String getQuery();

        // if you get schema from config
        @Config("columns")
        public SchemaConfig getColumns();
    }

    @Override
    public ConfigDiff transaction(ConfigSource config,
            InputPlugin.Control control)
    {
        PluginTask task = config.loadConfig(PluginTask.class);

        Schema schema = task.getColumns().toSchema();
        int taskCount = 1;

        return resume(task.dump(), schema, taskCount, control);
    }

    @Override
    public ConfigDiff resume(TaskSource taskSource,
            Schema schema, int taskCount,
            InputPlugin.Control control)
    {
        control.run(taskSource, schema, taskCount);
        return Exec.newConfigDiff();
    }

    @Override
    public void cleanup(TaskSource taskSource,
            Schema schema, int taskCount,
            List<TaskReport> successTaskReports)
    {
    }

    @Override
    public TaskReport run(TaskSource taskSource,
            Schema schema, int taskIndex,
            PageOutput output)
    {
        PluginTask task = taskSource.loadTask(PluginTask.class);

        BufferAllocator allocator = Exec.getBufferAllocator();
        PageBuilder pageBuilder = new PageBuilder(allocator, schema, output);

        // execute query
        QueryExecution qexec = null;
        try {
            Query query = QueryFactory.create(task.getQuery());
            qexec = QueryExecutionFactory.sparqlService(task.getEndpoint(), query);
            ResultSet results = qexec.execSelect();

            // return columns should be subset of query result columns.
            List<String> cols = results.getResultVars();
            Optional<Column> ngCol = schema.getColumns().stream()
                    .filter(col -> !cols.contains(col.getName()))
                    .findFirst();
            if (ngCol.isPresent()) {
                throw new NoSuchElementException(String.format("Sparql query do not return column %s", ngCol.get().getName()));
            }

            // result set loop
            while (results.hasNext()) {
                QuerySolution solution = results.next();

                for (Column col : schema.getColumns()) {
                    String colName = col.getName();
                    Type colType = col.getType();
                    int colIndex = col.getIndex();
                    if (colType.equals(Types.LONG)) {
                        pageBuilder.setLong(colIndex, getLongValue(solution, colName));
                    }
                    else if (colType.equals(Types.DOUBLE)) {
                        pageBuilder.setDouble(colIndex, getDoubleValue(solution, colName));
                    }
                    else if (colType.equals(Types.TIMESTAMP)) {
                        pageBuilder.setTimestamp(colIndex, getTimestampValue(solution, colName));
                    }
                    else {
                        pageBuilder.setString(colIndex, getStringValue(solution, colName));
                    }
                }
                pageBuilder.addRecord();
            }
        }
        finally {
            if (qexec != null) {
                qexec.close();
            }
        }

        pageBuilder.finish();
        return Exec.newTaskReport();
    }

    @Override
    public ConfigDiff guess(ConfigSource config)
    {
        return Exec.newConfigDiff();
    }

    // parse values by RDFDataTypes
    private String getStringValue(QuerySolution solution, String column)
    {
        if (solution.get(column).isLiteral()) {
            Literal literal = solution.getLiteral(column);
            return literal.getString();
        }
        return solution.get(column).toString();
    }

    private long getLongValue(QuerySolution solution, String column)
    {
        if (solution.get(column).isLiteral()) {
            Literal literal = solution.getLiteral(column);
            if (literal.getDatatype().getJavaClass() == BigInteger.class) {
                return literal.getInt();
            }
            else {
                return Long.parseLong(literal.toString());
            }
        }
        return Long.parseLong(solution.get(column).toString());
    }

    private double getDoubleValue(QuerySolution solution, String column)
    {
        if (solution.get(column).isLiteral()) {
            Literal literal = solution.getLiteral(column);
            if (literal.getDatatype().getJavaClass() == BigDecimal.class) {
                return literal.getDouble();
            }
            else {
                return Double.parseDouble(literal.toString());
            }
        }
        return Double.parseDouble(solution.get(column).toString());
    }

    private org.embulk.spi.time.Timestamp getTimestampValue(QuerySolution solution, String column)
    {
        String stringValue = getStringValue(solution, column);
        DateTimeZone defaultTZ = DateTimeZone.getDefault();
        DateTimeZone.setDefault(DateTimeZone.UTC);
        DateTime dt = DateTime.parse(stringValue);
        org.embulk.spi.time.Timestamp ts = org.embulk.spi.time.Timestamp.ofEpochMilli(dt.getMillis());
        DateTimeZone.setDefault(defaultTZ);
        return ts;
    }
}
