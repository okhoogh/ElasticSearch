package cn;

import cn.domain.Article;
import cn.repositories.ArticleRepository;
import org.elasticsearch.index.query.QueryBuilders;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.elasticsearch.core.ElasticsearchTemplate;
import org.springframework.data.elasticsearch.core.query.NativeSearchQuery;
import org.springframework.data.elasticsearch.core.query.NativeSearchQueryBuilder;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.List;
import java.util.Optional;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration("classpath:applicationContext.xml")
public class SpringDataElasticSearchTest {
    @Autowired
    private ArticleRepository articleRepository;

    @Autowired
    private ElasticsearchTemplate template;

    @Test
    public void createIndex() {
        // 创建索引，并配置映射关系
        template.createIndex(Article.class);
    }

    @Test
    public void addDocument() {
        for (int i = 10; i < 20; i++) {

            Article article = new Article();
            article.setId(i);
            article.setTitle(i + "全人类都应该感谢他，若不是这名士兵，世界人口或将减少30亿");
            article.setContent(i + "在历史上有一位全人类都应该感谢的士兵，要不是他，这个世界上的人口或将减少30亿，这个人就是前苏联军官斯坦尼斯拉夫·彼得罗夫。");
            articleRepository.save(article);
        }
    }

    @Test
    public void deleteDocumentById() {
        articleRepository.deleteById(1L);
    }

    @Test
    public void findAll() {
        Iterable<Article> articles = articleRepository.findAll();
        articles.forEach(article -> System.out.println(article));
    }

    @Test
    public void findById() {
        Optional<Article> optional = articleRepository.findById(1L);
        Article article = optional.get();
        System.out.println(article);
    }

    @Test
    public void findByTitle() {
        List<Article> list = articleRepository.findByTitle("全人类");
        list.forEach(article -> System.out.println(article));
    }

    @Test
    public void findByTitleOrContent() {
        Pageable pageable = PageRequest.of(0, 5);
        List<Article> articles = articleRepository.findByTitleOrContent("全人类", "操作", pageable);
        articles.forEach(article -> System.out.println(article));
    }

    @Test
    public void NativeSearchQuery() {
        NativeSearchQuery query = new NativeSearchQueryBuilder()
                .withQuery(QueryBuilders.queryStringQuery("全人类的手机").defaultField("content"))
                .withPageable(PageRequest.of(0, 12))
                .build();
        List<Article> list = template.queryForList(query, Article.class);
        list.forEach(article -> System.out.println(article));
    }
}
