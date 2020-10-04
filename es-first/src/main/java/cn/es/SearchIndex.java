package cn.es;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.Before;
import org.junit.Test;

import java.net.InetAddress;
import java.util.Iterator;
import java.util.Map;

public class SearchIndex {
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

    public void Search(QueryBuilder queryBuilder) {
        // 3、使用client执行查询
        SearchResponse searchResponse = client.prepareSearch("index_hello")
                .setTypes("article")
                .setQuery(queryBuilder)
                // 设置从第几页开始查询
                .setFrom(0)
                // 每页显示的行数
                .setSize(5)
                .get();
        // 4、得到查询的结果。
        SearchHits hits = searchResponse.getHits();
        // 5、取查询结果的总记录数
        System.out.println("查询结果的总记录数：" + hits.getTotalHits());
        // 6、取查询结果列表
        Iterator<SearchHit> iterator = hits.iterator();
        while (iterator.hasNext()) {
            SearchHit hit = iterator.next();
            // 打印文档对象，以json格式输出
            System.out.println(hit.getSourceAsString());
            // 取文档的属性
            System.out.println("----文档的属性----");
            Map<String, Object> document = hit.getSource();
            System.out.println(document.get("id"));
            System.out.println(document.get("title"));
            System.out.println(document.get("content"));

        }
        // 7、关闭client
        client.close();
    }

    @Test
    public void SearchById() throws Exception {
        // 1、创建一个Client对象
        // 2、创建一个查询对象，可以使用QueryBuilders工具类创建QueryBuilder对象。
        QueryBuilder queryBuilder = QueryBuilders.idsQuery().addIds("1", "3");
        Search(queryBuilder);
    }

    @Test
    public void SearchByTerm() throws Exception {
        // 1、创建一个Client对象
        // 2、创建一个查询对象，可以使用QueryBuilders工具类创建QueryBuilder对象。
        // 参数1：要搜索的字段   参数2：要搜索的关键词
        QueryBuilder queryBuilder = QueryBuilders.termQuery("title", "服务器");
        Search(queryBuilder);
    }

    @Test
    public void QueryStringSearch() throws Exception {
        QueryBuilder queryBuilder = QueryBuilders.queryStringQuery("阿富汗").defaultField("title");
        Search(queryBuilder, "title");
    }

    public void Search(QueryBuilder queryBuilder, String highlightField) {
        HighlightBuilder highlightBuilder = new HighlightBuilder();
        highlightBuilder.field(highlightField);
        highlightBuilder.preTags("<em>");
        highlightBuilder.postTags("</em>");

        // 3、使用client执行查询
        SearchResponse searchResponse = client.prepareSearch("index_hello")
                .setTypes("article")
                .setQuery(queryBuilder)
                // 设置从第几页开始查询
                .setFrom(0)
                // 每页显示的行数
                .setSize(5)
                .highlighter(highlightBuilder)
                .get();
        // 4、得到查询的结果。
        SearchHits hits = searchResponse.getHits();
        // 5、取查询结果的总记录数
        System.out.println("查询结果的总记录数：" + hits.getTotalHits());
        // 6、取查询结果列表
        Iterator<SearchHit> iterator = hits.iterator();
        while (iterator.hasNext()) {
            SearchHit hit = iterator.next();
            // 打印文档对象，以json格式输出
            System.out.println(hit.getSourceAsString());
            // 取文档的属性
            System.out.println("----文档的属性----");
            Map<String, Object> document = hit.getSource();
            System.out.println(document.get("id"));
            System.out.println(document.get("title"));
            System.out.println(document.get("content"));
            System.out.println("-------高亮结果--------");
            Map<String, HighlightField> highlightFields = hit.getHighlightFields();
            System.out.println(highlightFields);
            // 取title高亮后的结果
            HighlightField field = highlightFields.get(highlightField);
            Text[] fragments = field.getFragments();
            if(fragments != null) {
                String s = fragments[0].toString();
                System.out.println(s);
            }

        }
        // 7、关闭client
        client.close();
    }
}
