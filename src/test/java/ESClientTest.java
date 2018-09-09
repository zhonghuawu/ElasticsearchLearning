import com.huaa.Utils.GsonUtil;
import com.google.common.collect.Lists;
import com.huaa.learning.ESClient;
import com.huaa.learning.data.Blog;
import com.huaa.learning.db.ESUtil;
import org.apache.log4j.Logger;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.junit.Test;

/**
 * Desc:
 *
 * @author Huaa
 * @date 2018/8/19 20:09
 */

public class ESClientTest {

    private static Logger log = Logger.getLogger(ESClientTest.class);

    private static String index = "website";
    private static String type = "blog";
    private static long id = 4;


    @Test
    public void storeBlog() {
        ESClient.storeBlog();
    }

    @Test
    public void storeBlog3() {
        ESClient.storeBlog3();
    }

    @Test
    public void getBlog() {
        ESClient.getBlog();
    }

    @Test
    public void updateBlog() {
        ESClient.updateBlog();
    }

    @Test
    public void deleteBlog() {
        ESClient.deleteBlog();
    }

    @Test
    public void searchAll() {
        SearchResponse response = ESUtil.searchAll(index);
        SearchHits hits = response.getHits();
        printHits(hits);
    }

    @Test
    public void search() {
        SearchResponse response = ESUtil.search(index, "views", 50L);
        printHits(response.getHits());
    }

    @Test
    public void termSearch() {
        SearchResponse response = ESUtil.termSearch(index, "views", 79);
        SearchHits hits = response.getHits();
        printHits(hits);
    }

    @Test
    public void termsSearch() {
        SearchResponse response = ESUtil.termsSearch(index, "views", Lists.newArrayList(79, 80, 81));
        SearchHits hits = response.getHits();
        printHits(hits);
    }

    @Test
    public void rangeSearch() {
        SearchResponse response = ESUtil.rangeSearch(index, "views", 2, 20);
        printHits(response.getHits());
    }

    @Test
    public void existedSearch() {
        SearchResponse response = ESUtil.existedSearch(index, "tags");
        printHits(response.getHits());
    }


    @Test
    public void prefixSearch() {
        SearchResponse response = ESUtil.prefixSearch(index, "text", "tex");
        printHits(response.getHits());
    }

    @Test
    public void wildcardSearch() {
        SearchResponse response = ESUtil.wildcardSearch(index, "text", "t*x*");
        printHits(response.getHits());
    }

    @Test
    public void fuzzySearch() {
        SearchResponse response = ESUtil.fuzzySearch(index, "text", "test");
        printHits(response.getHits());
    }

    @Test
    public void typeSearch() {
        SearchResponse response = ESUtil.typeSearch(index, "text");
        printHits(response.getHits());
    }

    @Test
    public void idsSearch() {
        SearchResponse response = ESUtil.idsSearch(index, "78");
        printHits(response.getHits());
    }




    private static void printHits(SearchHits hits) {
        for (SearchHit hit : hits) {
            String id = hit.getId();
            Blog blog = GsonUtil.fromJson(hit.getSourceAsString(), Blog.class);
            System.out.println(String.format("%s: %s", id, blog.toString()));
        }
    }
}
