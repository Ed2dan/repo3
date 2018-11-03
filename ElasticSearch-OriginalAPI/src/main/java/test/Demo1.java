package test;

import com.fasterxml.jackson.databind.ObjectMapper;
import entity.Article;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.Before;
import org.junit.Test;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;

public class Demo1 {

    private TransportClient client;


    @Before
    public void init() throws Exception {
        Settings build = Settings.builder().put("cluster.name", "my‐elasticsearch").build();
        client = new PreBuiltTransportClient(build)
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9300))
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9301))
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9302));
    }


    @Test
    public void test1() throws UnknownHostException {

        CreateIndexRequestBuilder index1 = client.admin().indices().prepareCreate("index1");
        CreateIndexResponse createIndexResponse = index1.get();
        client.close();
    }

    @Test
    public void test2() throws Exception {
        XContentBuilder builder = XContentFactory.jsonBuilder()
                .startObject()
                .startObject("article")
                .startObject("properties")
                .startObject("id")
                .field("type", "long")
                .field("store", true)
                .endObject()
                .startObject("title")
                .field("type", "text")
                .field("analyzer", "ik_smart")
                .field("store", true)
                .endObject()
                .startObject("content")
                .field("type", "text")
                .field("analyzer", "ik_smart")
                .field("store", true)
                .endObject()
                .endObject()
                .endObject()
                .endObject();

        client.admin().indices().preparePutMapping("index1").setSource(builder).setType("article").get();
        client.close();
    }

    @Test
    public void test3() throws Exception {
        XContentBuilder builder = XContentFactory.jsonBuilder()
                .startObject()
                .field("id", 1L)
                .field("title", "习近平同美国总统特朗普通电话")
                .field("content", "国家主席习近平1日应约同美国总统特朗普通电话。\n" +
                        "\n" +
                        "特朗普表示，我重视同习近平主席的良好关系，愿通过习近平主席向中国人民致以良好的祝愿。两国元首经常直接沟通非常重要，我们要保持经常联系。我期待着同习主席在阿根廷二十国集团领导人峰会期间再次会晤，我们可以就一些重大问题进行深入探讨。希望双方共同努力，为我们的会晤做好充分准备。美方重视美中经贸合作，愿继续扩大对华出口。两国经济团队有必要加强沟通磋商。我支持美国企业积极参加首届中国国际进口博览会。\n" +
                        "\n" +
                        "习近平表示，很高兴再次同总统先生通电话。中方已就中美关系多次阐明原则立场。希望双方按照我同总统先生达成的重要共识，促进中美关系健康稳定发展。我也重视同总统先生的良好关系，愿同总统先生在出席阿根廷二十国集团领导人峰会期间再次会晤，就中美关系及其他重大问题深入交换意见。我们两人对中美关系健康稳定发展、扩大中美经贸合作都有良好的愿望，我们要努力把这种愿望变为现实。\n" +
                        "\n" +
                        "习近平指出，中美经贸合作的本质是互利共赢。过去一段时间，中美双方在经贸领域出现一些分歧，两国相关产业和全球贸易都受到不利影响，这是中方不愿看到的。中国即将举办首届国际进口博览会，这显示了中方增加进口、扩大开放的积极意愿。很高兴众多美国企业踊跃参与。中美双方也有通过协调合作解决经贸难题的成功先例。两国经济团队要加强接触，就双方关切问题开展磋商，推动中美经贸问题达成一个双方都能接受的方案。\n" +
                        "\n" +
                        "两国元首还就朝鲜半岛局势交换意见。习近平强调，今年以来，朝鲜半岛形势出现积极变化。中方赞赏总统先生同金正恩委员长举行历史性会晤，推动了朝鲜半岛无核化和政治解决进程。希望美朝双方照顾彼此关切，进一步推进朝鲜半岛无核化和构建朝鲜半岛和平机制进程。中方将继续发挥建设性作用。\n" +
                        "\n" +
                        "特朗普表示，今年以来，美朝会谈取得了积极进展。美方高度重视中方在朝鲜半岛问题上的重要作用，愿继续同中方加强沟通协调。")
                .endObject();
        client.prepareIndex("index1", "article", "1").setSource(builder).get();
        client.close();
    }

    @Test
    public void test4() throws Exception {
        Article article = new Article();
        article.setId(2L);
        article.setTitle("再挥大棒!特朗普签署行政令,制裁委内瑞拉黄金出口");
        article.setContent("海外网11月2日电 9月以来，美国对委内瑞拉采取一系列制裁措施，包括委内瑞拉总统马杜罗及其配偶、主要内阁官员等均被列入个人制裁名单中，两国关系的紧张程度持续升级。当地时间周四（1日），白宫发布的一封信件中称，美国总统特朗普已签署行政令，将对委内瑞拉的黄金出口实施全面制裁。\n" +
                "\n" +
                "综合俄罗斯卫星通讯社和美国之音报道，特朗普在给国会参众两院领袖的信中写道：“我特此报告，我已经签署行政令，将在2015年3月8日生效的第13692号行政令之外，对委内瑞拉采取额外措施。”他补充道，这次制裁主要针对委内瑞拉黄金部门的财产和利益，以及任何与委内瑞拉政府有牵连的部门。");
        String json = new ObjectMapper().writeValueAsString(article);
        client.prepareIndex("index1", "article", "2").setSource(json, XContentType.JSON).get();
        client.close();
    }

    @Test
    public void test5() throws Exception {
        TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery("title", "重庆公园");
        search(termQueryBuilder);
    }

    private void search(QueryBuilder queryBuilder) {
        SearchResponse searchResponse = client.prepareSearch("index1").setTypes("article").setQuery(queryBuilder).setFrom(0).setSize(20).get();
        SearchHits hits = searchResponse.getHits();
        System.out.println("总记录数为" + hits.getTotalHits());
        for (SearchHit hit : hits) {
            String sourceAsString = hit.getSourceAsString();
            System.out.println(sourceAsString);
            Map<String, Object> source = hit.getSource();
            Object id = source.get("id");
            Object title = source.get("title");
            Object content = source.get("content");
            System.out.println(id + "  " + title + "  " + content);
        }
        client.close();
    }

    @Test
    public void test6() {
        IdsQueryBuilder article = QueryBuilders.idsQuery("article").addIds("2");
        search(article);
    }

    @Test
    public void test7() throws Exception {
        ObjectMapper objectMapper = new ObjectMapper();
        Article article = new Article();
        for (int i = 3; i < 100; i++) {
            article.setId((long) i);
            article.setTitle("重庆万州公交车坠江原因：乘客司机互殴致车辆失控" + i);
            article.setContent("解放战争中，造手榴弹最强的竟是这一解放区" + i);
            String s = objectMapper.writeValueAsString(article);
            client.prepareIndex("index1", "article", i + "").setSource(s, XContentType.JSON).get();
        }
        client.close();
    }

    @Test
    public void test8() {
        QueryStringQueryBuilder queryStringQueryBuilder = QueryBuilders.queryStringQuery("重庆公园").defaultField("title");
        search(queryStringQueryBuilder);
    }

    @Test
    public void test9() {
        QueryStringQueryBuilder queryStringQueryBuilder = QueryBuilders.queryStringQuery("原因").defaultField("title");
        searchForHighlight(queryStringQueryBuilder);

    }

    private void searchForHighlight(QueryBuilder queryBuilder) {
        HighlightBuilder highlightBuilder = new HighlightBuilder();
        highlightBuilder.preTags("<em>");
        highlightBuilder.postTags("</em>");
        highlightBuilder.field("title");

        SearchRequestBuilder searchRequestBuilder = client.prepareSearch("index1")
                .setTypes("article")
                .setFrom(0)
                .setSize(20)
                .highlighter(highlightBuilder)
                .setQuery(queryBuilder);
        SearchResponse searchResponse = searchRequestBuilder.get();
        SearchHits hits = searchResponse.getHits();
        System.out.println("总记录数为" + hits.getTotalHits());
        for (SearchHit hit : hits) {
            Map<String, Object> document = hit.getSource();
            System.out.println(document.get("id"));
            System.out.println(document.get("title"));
            System.out.println(document.get("content"));
            Map<String, HighlightField> highlightFields = hit.getHighlightFields();
            System.out.println(highlightFields);
            HighlightField title = highlightFields.get("title");
            Text[] fragments = title.getFragments();
            for (Text fragment : fragments) {
                System.out.println(fragment.toString());
            }
        }
        client.close();

    }

    @Test
    public void test10() {
        client.prepareDelete("index1", "article", "99").get();
    }

    @Test
    public void test11() {
        QueryStringQueryBuilder queryStringQueryBuilder = QueryBuilders.queryStringQuery("台湾公园").defaultField("title");
        TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery("content", "美女");
        SearchResponse searchResponse = client.prepareSearch("index1").setTypes("article").setQuery(queryStringQueryBuilder).setPostFilter(termQueryBuilder).setFrom(0).setSize(100).get();
        SearchHits hits = searchResponse.getHits();
        System.out.println("总记录数为" + hits.getTotalHits());
        for (SearchHit hit : hits) {
            String sourceAsString = hit.getSourceAsString();
            System.out.println(sourceAsString);
            Map<String, Object> source = hit.getSource();
            Object id = source.get("id");
            Object title = source.get("title");
            Object content = source.get("content");
            System.out.println(id + "  " + title + "  " + content);
        }
        client.close();
    }
}
