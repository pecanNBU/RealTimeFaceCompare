package com.hzgc.service.staticreposuite;

import com.hzgc.dubbo.staticrepo.ObjectInfoTable;
import com.hzgc.service.staticrepo.ElasticSearchHelper;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.Test;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;
import java.util.UUID;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

public class EsDataSuite {
    @Test
    public void testPutFloatArrayToEs() throws IOException {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSZ");
        sdf.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"));
        Client client = ElasticSearchHelper.getEsClient();
        for (int i = 0; i < 100; i++) {
            String uuid = UUID.randomUUID().toString();
            IndexResponse response = client.prepareIndex("dynamic-v2", "person", uuid)
                    .setSource(jsonBuilder()
                            .startObject()
                            .field("exacttime", sdf.format(new Date()))
                            .field("ipcid", "test")
                            .endObject()).get();

            String _index = response.getIndex();
            String _type = response.getType();
            String _id = response.getId();
            long _version = response.getVersion();
            RestStatus status = response.status();
            System.out.println("_index: " + _index + ", _type: " + _type + ", _id： "
                    + _id + "_version: " + _version + "status: " + status);
        }

    }

    @Test
    public void testScrollsSearch() {
        Client client = ElasticSearchHelper.getEsClient();
        SearchRequestBuilder builder = client.prepareSearch(ObjectInfoTable.TABLE_NAME)
                .setTypes(ObjectInfoTable.PERSON_COLF)
                .addSort(FieldSortBuilder.DOC_FIELD_NAME, SortOrder.ASC)
                .setScroll(new TimeValue(300000))
                .setExplain(true).setSize(2000);
        QueryBuilder qb = QueryBuilders.matchQuery("name", "花");
        builder.setQuery(qb);
        SearchResponse response = builder.get();
        do {
            System.out.println("Search  total " + response.getHits().getTotalHits());
            response = client.prepareSearchScroll(response.getScrollId())
                    .setScroll(new TimeValue(300000))
                    .execute()
                    .actionGet();
        } while (response.getHits().getHits().length != 0);
    }

    @Test
    public void testGetResultOfDynamicRepo() {
        QueryBuilder qb = QueryBuilders.matchAllQuery();
        Client client = ElasticSearchHelper.getEsClient();
        SearchRequestBuilder builder = client.prepareSearch("dynamic")
                .setTypes("person")
                .addSort(FieldSortBuilder.DOC_FIELD_NAME, SortOrder.ASC)
                .setQuery(qb)
                .setScroll(new TimeValue(6000))
                .setExplain(true).setSize(2000);
        SearchResponse response = builder.get();
        long total_time = 0;
        do {
            long start_time = System.currentTimeMillis();
            System.out.println("Search  total " + response.getHits().getHits().length);
            SearchHit[] hits = response.getHits().getHits();
            for (SearchHit hit : hits) {
                String time = (String) hit.getSource().get("t");
                if (time == null) {
                    System.out.println(time + ": " + hit.getId());
                }
            }
            long end_time = System.currentTimeMillis();
            total_time += (end_time - start_time);
            response = client.prepareSearchScroll(response.getScrollId())
                    .setScroll(new TimeValue(6000))
                    .execute()
                    .actionGet();
        } while (response.getHits().getHits().length != 0);
        System.out.println(total_time);

    }

    //    @Test
//    public void testGetObjcetInfo(){
//        ObjectInfoHandlerImpl handler = new ObjectInfoHandlerImpl();
//        Map<String, Map<String, Object>> objectSearchResult = ObjectInfoHandlerImpl.getAllObjectInfo();
//    }
    @Test
    public void testConn() {
        System.out.println("".length());
    }

    @Test
    /*
    delete the index of big image from elasticsearch
     */
    public void DeleteBigImgIndex() {
       /* BulkRequestBuilder bulkRequest = ElasticSearchHelper.getEsClient().prepareBulk();
        RealTimeCompare realTimeCompare = new RealTimeCompare();
        SearchOption option = new SearchOption();
        option.setThreshold(0.00f);
        option.setSearchType(SearchType.PERSON);
        List<String> ImageIdList = realTimeCompare.getImageIdListFromEs(option);
        List<String> ImageIdFilterList = ImageIdList.parallelStream().filter(id -> id.endsWith("_00")).collect(Collectors.toList());
        System.out.println(ImageIdFilterList.size());
        for (String aImageIdFilterList : ImageIdFilterList) {
            bulkRequest.add(ElasticSearchHelper.getEsClient().prepareDelete(DynamicTable.DYNAMIC_INDEX, DynamicTable.PERSON_INDEX_TYPE, aImageIdFilterList).request());
        }

        BulkResponse bulkResponse = bulkRequest.get();
        if (bulkResponse.hasFailures()) {
            for (BulkItemResponse item : bulkResponse.getItems()) {
                System.out.println(item.getFailureMessage());
            }
        } else {
            System.out.println("delete ok");
        }*/
    }
}
