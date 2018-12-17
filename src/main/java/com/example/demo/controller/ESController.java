package com.example.demo.controller;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by Administrator on 2018/12/15.
 */
@Controller
public class ESController {

    @Autowired
    private RestHighLevelClient client;

    /**
     *
     * 功能描述: 增加数据
     *
     */
    @PostMapping("person/add")
    public ResponseEntity add() {
        Map<String, Object> jsonMap = new HashMap<>();
        jsonMap.put("user", "kimchy");
        jsonMap.put("postDate", new Date());
        jsonMap.put("message", "trying out Elasticsearch");
        IndexRequest request = new IndexRequest("posts", "doc", "1")
                .source(jsonMap);


        ActionListener<IndexResponse> actionListener = new ActionListener<IndexResponse>() {
            @Override
            public void onResponse(IndexResponse indexResponse) {
                System.out.printf("成功");

            }

            @Override
            public void onFailure(Exception e) {
                System.out.printf("失败");
            }
        };

        try {
            client.indexAsync(request, RequestOptions.DEFAULT,actionListener);
        } catch (Exception e) {
            e.printStackTrace();
        }


        return new ResponseEntity("成功",HttpStatus.OK) ;

    }

//    /**
//     * 功能描述: 查找数据
//     *
//     */
    @GetMapping("person/get")
    public ResponseEntity get() throws IOException {
        GetRequest getRequest = new GetRequest(
                "posts",
                "doc",
                "1");
        GetResponse documentFields = client.get(getRequest, RequestOptions.DEFAULT);
        return new ResponseEntity(documentFields.getSource(),HttpStatus.OK);

    }


    @DeleteMapping("person/delete")
    public ResponseEntity delete(){

        DeleteRequest deleteRequest = new DeleteRequest("posts","doc","1");
        try {
            DeleteResponse delete = client.delete(deleteRequest, RequestOptions.DEFAULT);
            return new ResponseEntity("删除成功",HttpStatus.OK);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return new ResponseEntity("删除失败",HttpStatus.OK);
    }



//
//    /**
//     *
//     * 功能描述: 删除数据
//     *
//     */
    @PutMapping("person/update")
    public ResponseEntity update() {
        try {
            UpdateRequest request = new UpdateRequest(
                    "posts",
                    "doc",
                    "1");
            Map<String, Object> jsonMap = new HashMap<>();
            jsonMap.put("updated", new Date());
            jsonMap.put("reason", "daily update");
            request.doc(jsonMap);
            client.update(request,RequestOptions.DEFAULT);
            return new ResponseEntity("成功", HttpStatus.OK);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return new ResponseEntity("失败", HttpStatus.OK);
    }





    @GetMapping("person/bulk")
    public ResponseEntity bulk() {
        try {

            BulkRequest bulkRequest = new BulkRequest();

            for (int i = 0; i <100 ; i++) {
                Map<String, Object> jsonMap = new HashMap<>();
                jsonMap.put("user", "kimchy"+i);
                jsonMap.put("postDate", new Date());
                jsonMap.put("message", "trying out Elasticsearch"+i);
                IndexRequest request = new IndexRequest("posts", "doc", i+"")
                        .source(jsonMap);
                bulkRequest.add(request);
            }

            client.bulk(bulkRequest);


            return new ResponseEntity("成功", HttpStatus.OK);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return new ResponseEntity("失败", HttpStatus.OK);
    }



    @GetMapping("person/deletes")
    public ResponseEntity deletes(){
            try {
                DeleteByQueryRequest request = new DeleteByQueryRequest("source1", "source2");
//                request.setIndicesOptions(IndicesOptions.);
                request.setDocTypes("doc");
                client.deleteByQuery(request,RequestOptions.DEFAULT);
            }catch (Exception e){
            }
        return new ResponseEntity("",HttpStatus.OK);
    }





    @RequestMapping("person/search")
    @ResponseBody
    public SearchResponse search(){
        try {
            SearchRequest searchRequest = new SearchRequest();
            searchRequest.indices("megacorp");
//            MatchQueryBuilder matchQueryBuilder = new MatchQueryBuilder("first_name","Jane");
//            matchQueryBuilder.fuzziness(Fuzziness.AUTO);

            //MatchPhraseQueryBuilder matchPhraseQueryBuilder = new MatchPhraseQueryBuilder("about","rock climbing");

            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
//            searchSourceBuilder.from(0);
//            searchSourceBuilder.size(3);
//            searchSourceBuilder.sort(new FieldSortBuilder("age").order(SortOrder.ASC));
            //searchSourceBuilder.query(matchQueryBuilder);
            //searchSourceBuilder.query(matchPhraseQueryBuilder);



            //searchSourceBuilder.fetchSource(false);
//            String[] includeFields = new String[] {"first_name"};
//            String[] excludeFields = new String[] {"_type"};
//            searchSourceBuilder.fetchSource(includeFields, excludeFields);

            // 高亮
//            HighlightBuilder highlightBuilder = new HighlightBuilder();
//            HighlightBuilder.Field highlightTitle = new HighlightBuilder.Field("about");
//            //HighlightBuilder.Field highlightUser = new HighlightBuilder.Field("last_name");
////            highlightTitle.highlighterType("unified");
//            highlightBuilder.field(highlightTitle);
//           // highlightBuilder.field(highlightUser);
//            searchSourceBuilder.highlighter(highlightBuilder);


            // 聚合
//            TermsAggregationBuilder aggregation = AggregationBuilders.terms("by_company")
//                    .field("interests.keyword");
//            aggregation.subAggregation(AggregationBuilders.avg("average_age")
//                    .field("age"));
//            searchSourceBuilder.aggregation(aggregation);


            // 建议
//            SuggestionBuilder termSuggestionBuilder = SuggestBuilders.termSuggestion("user").text("kmichy");
//            SuggestBuilder suggestBuilder = new SuggestBuilder();
//            suggestBuilder.addSuggestion("suggest_user", termSuggestionBuilder);
//            searchSourceBuilder.suggest(suggestBuilder);

//            searchSourceBuilder.query(termSuggestionBuilder);
            searchRequest.source(searchSourceBuilder);

            System.out.println(searchRequest.source().toString());

            SearchResponse search = client.search(searchRequest, RequestOptions.DEFAULT);
//            List<Map<String,Object>> list = new ArrayList<>();
//            SearchHits hits = search.getHits();
//            SearchHit[] hits1 = hits.getHits();
//            for (SearchHit documentFields : hits1) {
//                Map<String, Object> sourceAsMap = documentFields.getSourceAsMap();
//                list.add(sourceAsMap);
//            }

            System.out.println("");
            return search;

        }catch (Exception e){

            e.printStackTrace();

        }
        return null;
    }




    @RequestMapping("/searchScroll")
    @ResponseBody
    public Map<String,Object> searchScroll(@RequestParam("scrollId") String scrollId){
        SearchResponse searchResponse = null;
        Map<String,Object> map = new HashMap<>();
        SearchHits hits=null;
        try {
            if (StringUtils.isEmpty(scrollId)){
                SearchRequest searchRequest = new SearchRequest("megacorp");
                SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
//            MatchQueryBuilder matchQueryBuilder = new MatchQueryBuilder("first_name", "Jane");
//            searchSourceBuilder.query(matchQueryBuilder);
                searchSourceBuilder.size(2);
                searchRequest.source(searchSourceBuilder);
                searchRequest.scroll(TimeValue.timeValueMinutes(1L));
                searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
                scrollId = searchResponse.getScrollId();
                hits = searchResponse.getHits();
            }else {
                SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId);
                scrollRequest.scroll(TimeValue.timeValueSeconds(30));
                searchResponse = client.scroll(scrollRequest, RequestOptions.DEFAULT);
                scrollId = searchResponse.getScrollId();
                hits = searchResponse.getHits();
            }
            map.put("hits",hits);
            map.put("scrollId",scrollId);
            System.out.println(scrollId);
            return map;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }


    public static void main(String[] args) {
        TimeValue timeValue = TimeValue.timeValueMinutes(1L);

    }


}
