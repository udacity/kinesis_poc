package poc.kinesis.bolt;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.*;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.*;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;


public class RDSWriterBolt extends BaseRichBolt {
    private static final Logger LOG = LoggerFactory.getLogger(RDSWriterBolt.class);
    private transient OutputCollector collector;
    private Table ddTable;
    private String jdbcUrl;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;

        AmazonDynamoDB client = new AmazonDynamoDBClient();
        client.setEndpoint("dynamodb.us-west-2.amazonaws.com");
        DynamoDB dynamoDB = new DynamoDB(client);
        ddTable = dynamoDB.getTable("workspace_stat");

        LOG.info("Creating JDBC Connection String");
        String hostname = "rds-dev.cpvqcxvhbkmc.us-west-2.rds.amazonaws.com";
        int port = 5432;
        String dbName = "RDS_DEV";
        String userName = "dev";
        String password = "";
        jdbcUrl = "jdbc:postgresql://" + hostname + ":" + port + "/" + dbName + "?user=" + userName + "&password=" + password;
        try {
            Class.forName("org.postgresql.Driver");
        } catch (ClassNotFoundException e) {
            LOG.error("Drive Class not found", e);
        }
    }

    @Override
    public void execute(Tuple tuple) {
        LOG.info("Start process tuple in RDS writer");
        String date = LocalDate.now(ZoneId.of("UTC")).minusDays(3).format(DateTimeFormatter.ISO_DATE);

        Map<String, Object> expressionAttributeValues = new HashMap<String, Object>();
        expressionAttributeValues.put(":today", date);

        ItemCollection<ScanOutcome> items = ddTable.scan("begins_with(id, :today)", // FilterExpression
        null,
                null,
                expressionAttributeValues);

        LOG.info("Scan of " + ddTable.getTableName() + " for items today "+date);
        Iterator<Item> iterator = items.iterator();

        Connection connection = null;
        try {
            connection = DriverManager.getConnection(jdbcUrl);
        } catch (SQLException e) {
            LOG.error("Connection Failed! Check output console", e);
            return;
        }
        if (connection != null) {
            LOG.info("You made it, take control your database now!");
        } else {
            LOG.info("Failed to make connection!");
        }

        while (iterator.hasNext()) {
            HashMap<String, Object> dataMap = new HashMap<>();
            Map<String, Object> values = iterator.next().asMap();
            try {
                String[] id = ((String) values.get("id")).split("_");
                dataMap.put("day_of", LocalDate.parse(id[0].replace("T", "")));
                dataMap.put("nd_key", id[1]);
                dataMap.put("nd_version", id[2]);
                dataMap.put("nd_locale", id[3]);
                dataMap.put("workspace_id", id[4]);
                dataMap.put("concept_key", id[5]);

                dataMap.put("count_total_views", getInt(WorkspaceWriterBolt.WorkspaceEvent.VIEWED.getEventCount(), values));
                dataMap.put("count_viewed_users", getSetSize(WorkspaceWriterBolt.WorkspaceEvent.VIEWED.getUserSet(), values));

                dataMap.put("count_sessions_interacted", getSetSize(WorkspaceWriterBolt.WorkspaceEvent.INTERACTED.getEventCount(), values));
                dataMap.put("count_interacted_users", getSetSize(WorkspaceWriterBolt.WorkspaceEvent.INTERACTED.getUserSet(), values));
                dataMap.put("sum_total_time_sec", getTotalFromMap("workspace_interacted", WorkspaceWriterBolt.WorkspaceEvent.INTERACTED.getTotalTimeSec(), values));
                dataMap.put("sum_interactions", getTotalFromMap("workspace_interacted", WorkspaceWriterBolt.WorkspaceEvent.INTERACTED.getNumInteractions(), values));

                dataMap.put("count_terminals_added", getInt(WorkspaceWriterBolt.WorkspaceEvent.TERMINAL_ADDED.getEventCount(), values));
                dataMap.put("count_terminal_added_users", getSetSize(WorkspaceWriterBolt.WorkspaceEvent.TERMINAL_ADDED.getUserSet(), values));

                dataMap.put("count_terminals_removed", getInt(WorkspaceWriterBolt.WorkspaceEvent.TERMINAL_REMOVED.getEventCount(), values));
                dataMap.put("count_terminal_removed_users", getSetSize(WorkspaceWriterBolt.WorkspaceEvent.TERMINAL_REMOVED.getUserSet(), values));

                dataMap.put("count_previews_opened", getInt(WorkspaceWriterBolt.WorkspaceEvent.CODE_PREVIEWED.getEventCount(), values));
                dataMap.put("count_preview_opened_users", getSetSize(WorkspaceWriterBolt.WorkspaceEvent.CODE_PREVIEWED.getUserSet(), values));

                dataMap.put("count_submit_clicks", getInt(WorkspaceWriterBolt.WorkspaceEvent.SUBMIT_PROJECT_CLICKED.getEventCount(), values));
                dataMap.put("count_submit_click_users", getSetSize(WorkspaceWriterBolt.WorkspaceEvent.SUBMIT_PROJECT_CLICKED.getUserSet(), values));

                dataMap.put("count_projects_submitted", getInt(WorkspaceWriterBolt.WorkspaceEvent.PROJECT_SUBMITTED.getEventCount(), values));
                dataMap.put("count_project_submitted_users", getSetSize(WorkspaceWriterBolt.WorkspaceEvent.PROJECT_SUBMITTED.getUserSet(), values));

                dataMap.put("count_code_reset_clicks", getInt(WorkspaceWriterBolt.WorkspaceEvent.CODE_RESET_CLICKED.getEventCount(), values));
                dataMap.put("count_code_reset_click_users", getSetSize(WorkspaceWriterBolt.WorkspaceEvent.CODE_RESET_CLICKED.getUserSet(), values));

                dataMap.put("count_code_resets", getInt(WorkspaceWriterBolt.WorkspaceEvent.CODE_RESET.getEventCount(), values));
                dataMap.put("count_code_reset_users", getSetSize(WorkspaceWriterBolt.WorkspaceEvent.CODE_RESET.getUserSet(), values));

                LOG.info("Data Map: " + dataMap.toString());

                //Prepare SQL Statement
                StringBuilder sql = new StringBuilder("INSERT INTO ").append("test_tao.workspace_summary").append(" (");
                StringBuilder placeholders = new StringBuilder();

                for (Iterator<String> iter = dataMap.keySet().iterator(); iter.hasNext(); ) {
                    sql.append(iter.next());
                    placeholders.append("?");

                    if (iter.hasNext()) {
                        sql.append(",");
                        placeholders.append(",");
                    }
                }

                sql.append(") VALUES (").append(placeholders).append(")");
                LOG.info("SQL Statement: " + sql.toString());

                PreparedStatement preparedStatement = connection.prepareStatement(sql.toString());
                int i = 1;
                for (Object value : dataMap.values()) {
                    preparedStatement.setObject(i++, value);
                }

                // execute select SQL stetement
                preparedStatement.executeQuery();

            }catch (Exception ex){
                LOG.error("DB entry with issues: "+values.toString(), ex);
            }
        }
        collector.ack(tuple);
    }


    private int getInt(String fieldName, Map<String, Object> values){
        if(values.containsKey(fieldName)){
             return (int) values.get(fieldName);
        }
        return 0;
    }

    private int getSetSize(String fieldName, Map<String, Object> values){
        if(values.containsKey(fieldName)){
            return ((Set)values.get(fieldName)).size();
        }
        return 0;
    }

    private double getTotalFromMap(String mapField, String numberField, Map<String, Object> values){
        double total = 0;
        if(values.containsKey(mapField)) {
            Map<String, Map<String, BigDecimal>> deeperMap = (Map<String, Map<String, BigDecimal>>)values.get(mapField);
            for (Map<String, BigDecimal> stats: deeperMap.values()) {
                total = total + stats.get(numberField).doubleValue();
            }
        }
        return total;
    }

    private String buildStatement(){
        String updateTable = "INSERT INTO test_tao.workspace_summary (id, name, age) VALUES() ON CONFLICT DO UPDATE ";



        /*
                try {
            Connection connection = DriverManager.getConnection(jdbcUrl);
            Statement updateStatement = connection.createStatement();
            String updateTable = "INSERT INTO test_tao.workspace_summary (id, name, age) VALUES() ON CONFLICT DO UPDATE ";
            updateStatement.execute(updateTable);
            updateStatement.close();
        } catch (SQLException e) {
            LOG.error("DB connection failed", e);
        }

         */
        return  updateTable;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
