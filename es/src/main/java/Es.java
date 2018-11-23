import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.config.ConfigurationSource;
import org.apache.logging.log4j.core.config.Configurator;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.io.*;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class Es {
    public static Client connectEs(String ip) throws UnknownHostException {
        Settings settings = Settings.builder()
                //集群名称
                .put("cluster.name", "onesearch")
                //自动嗅探
                .put("client.transport.sniff", true)
                .put("discovery.type", "zen")
                .put("discovery.zen.minimum_master_nodes", 1)
                .put("discovery.zen.ping_timeout", "500ms")
                .put("discovery.initial_state_timeout", "500ms")
                .build();
        //创建client  因为是默认的集群名称，所以   settings.empty        端口号不能与HTTP设置的端口号一致
        Client client = new PreBuiltTransportClient(Settings.EMPTY)
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(ip), 9300));
        System.out.println("connect succ");
        return client;
    }


    public static void createData(Client client) {
        Map<String, Object> map = new HashMap<String, Object>();
        // map.put("name", "Smith Wang");
        map.put("name", "Smith Chen");
        // map.put("age", 20);
        map.put("age", 5);
        // map.put("interests", new String[]{"sports","film"});
        map.put("interests", new String[] { "reading", "film" });
        // map.put("about", "I love to go rock music");
        map.put("about", "I love to go rock climbing");

        IndexResponse response = client.prepareIndex("megacorp", "employee", UUID.randomUUID().toString())
                .setSource(map).get();
        System.out.println("写入数据结果=" + response.status().getStatus() + "！id=" + response.getId());
    }

    public static void main(String[] args) throws IOException {

        File file = new File("/home/pp/IdeaProjects/es/src/log4j.xml");
        BufferedInputStream in = new BufferedInputStream(new FileInputStream(file));
        final ConfigurationSource source = new ConfigurationSource(in);
        Configurator.initialize(null, source);

        org.apache.logging.log4j.Logger logger = LogManager.getLogger("myLogger");
        Client client=Es.connectEs("127.0.0.1");
        Es.createData(client);
    }
}
