package cn.es;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.Before;
import org.junit.Test;

import java.net.InetAddress;

public class ElasticSearchClientTest {

    private TransportClient client;

    @Before
    public void init() throws Exception {
        // 1、创建一个Settings对象，相当于是一个配置信息。主要配置集群的名称。
        Settings settings = Settings.builder()
                .put("cluster.name", "my‐elasticsearch")
                .build();
        // 2、创建一个客户端Client对象
        client = new PreBuiltTransportClient(settings)
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9301))
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9302))
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9303));
    }

    @Test
    public void createIndex() throws Exception {
        // 1、创建一个Settings对象，相当于是一个配置信息。主要配置集群的名称。
        Settings settings = Settings.builder()
                .put("cluster.name", "my‐elasticsearch")
                .build();
        // 2、创建一个客户端Client对象
        TransportClient client = new PreBuiltTransportClient(settings);
        client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9301));
        client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9302));
        client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9303));
        // 3、使用client对象创建一个索引库
        client.admin().indices().prepareCreate("index_hello")
                .get();     // 执行操作
        // 4、关闭client对象
        client.close();
    }

    @Test
    public void setMappings() throws Exception {
        // 1、创建一个Settings对象，相当于是一个配置信息。主要配置集群的名称。
        Settings settings = Settings.builder()
                .put("cluster.name", "my‐elasticsearch")
                .build();
        // 2、创建一个客户端Client对象
        TransportClient client = new PreBuiltTransportClient(settings)
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9301))
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9302))
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9303));
        // 3、创建一个mapping信息，应该是一个json数据，可以是字符串，也可以是XContextBuilder对象
        /*
            {
              "article": {
                "properties": {
                  "id": {
                    "type": "long",
                    "store": true
                  },
                  "title": {
                    "type": "text",
                    "store": true,
                    "index": true,
                    "analyzer": "ik_smart"
                  },
                  "content": {
                    "type": "text",
                    "store": true,
                    "index": true,
                    "analyzer": "ik_smart"
                  }
                }
              }
            }
         */
        XContentBuilder builder = XContentFactory.jsonBuilder()
                .startObject()
                .startObject("article")
                .startObject("properties")
                .startObject("id")
                .field("type", "long")
                .field("store", "true")
                .endObject()
                .startObject("title")
                .field("type", "text")
                .field("store", "true")
                .field("index", true)
                .field("analyzer", "ik_smart")
                .endObject()
                .startObject("content")
                .field("type", "text")
                .field("store", "true")
                .field("index", true)
                .field("analyzer", "ik_smart")
                .endObject()
                .endObject()
                .endObject()
                .endObject();
        // 4、使用client把mapping信息设置到索引库中
        client.admin().indices()
                // 设置要做映射的索引
                .preparePutMapping("index_hello")
                // 设置要做映射的Type
                .setType("article")
                // mapping信息，可以是XContentBuilder对象或Json字符串
                .setSource(builder)
                .get();

        //  5、关闭client对象
        client.close();
    }

    @Test
    public void addDocument1() throws Exception {
        // 1、创建一个Settings对象
        // 2、创建一个Client对象
        /* init方法已完成 */

        // 3、创建一个文档对象，创建一个json格式的字符串，或者使用XContentBuilder
        XContentBuilder builder = XContentFactory.jsonBuilder()
                .startObject()
                    .field("id", 1)
                    .field("title", "ElasticSearch是一个基于Lucene的搜索服务器")
                    .field("content", "它提供了一个分布式多用户能力的全文搜索引擎，基于RESTful web接口。Elasticsearch是用Java开发的，并作为Apache许可条款下的开放源码发布，是当前流行的企业级搜索引擎。设计用于云计算中，能够达到实时搜索，稳定，可靠，快速，安装使用方便。")
                .endObject();

        // 4、使用Client对象吧文档添加到索引库中
        client.prepareIndex()
                // 设置索引
                .setIndex("index_hello")
                // 设置type
                .setType("article")
                // 设置id
                .setId("1")
                // 设置文档信息
                .setSource(builder)
                .get();
        // 5、关闭client
        client.close();
    }

    @Test
    public void addDocument2() throws Exception {
        // 1、创建一个pojo类
        Article article = new Article();
        article.setId(2);
        article.setTitle("addDocument2");
        article.setContent("addDocument2");
        // 2、使用工具类把pojo转换成json字符串
        ObjectMapper mapper = new ObjectMapper();
        String json = mapper.writeValueAsString(article);
        System.out.println(json);
        // 3、把文档写入索引库
        client.prepareIndex()
                .setIndex("index_hello")
                .setType("article")
                .setId("3")
                .setSource(json, XContentType.JSON)
                .get();
        // 4、关闭client
        client.close();
    }

    @Test
    public void addDocument3() throws Exception {
        for (int i = 4; i < 30; i++) {
            // 1、创建一个pojo类
            Article article = new Article();
            article.setId(i);
            article.setTitle("阿富汗东部汽车炸弹袭击致死15人" + i);
            article.setContent("10月3日，在阿富汗楠格哈尔省辛瓦尔地区，安全部队成员查看袭击现场。 阿富汗政府官员3日证实，阿东部楠格哈尔省当天发生一起汽车炸弹袭击，造成15人死亡、42人受伤。" + i);
            // 2、使用工具类把pojo转换成json字符串
            ObjectMapper mapper = new ObjectMapper();
            String json = mapper.writeValueAsString(article);
            System.out.println(json);
            // 3、把文档写入索引库
            client.prepareIndex()
                    .setIndex("index_hello")
                    .setType("article")
                    .setId("" + i)
                    .setSource(json, XContentType.JSON)
                    .get();
        }
        // 4、关闭client
        client.close();
    }
}
