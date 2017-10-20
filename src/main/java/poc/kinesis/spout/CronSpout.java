package poc.kinesis.spout;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import poc.kinesis.bolt.WorkspaceWriterBolt;

import java.util.Map;

public class CronSpout extends BaseRichSpout {
    protected static final Logger LOG = LoggerFactory.getLogger(CronSpout.class);
    private transient SpoutOutputCollector collector;
    private long previousTs = 0;
    private final static int interval = 60*60*1000;

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector = spoutOutputCollector;
    }

    @Override
    public void nextTuple() {
        long currentTs = System.currentTimeMillis();
        if(currentTs-previousTs>interval){
            LOG.warn("Cron Spout emit message");
            collector.emit(new Values(""));
            previousTs = currentTs;
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("cron"));
    }
}
