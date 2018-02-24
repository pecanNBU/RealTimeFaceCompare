package com.hzgc.service.clustering;

import com.hzgc.dubbo.clustering.AlarmInfo;
import com.hzgc.dubbo.clustering.ClusteringAttribute;
import com.hzgc.service.util.HBaseHelper;
import com.hzgc.util.common.ObjectUtil;
import com.hzgc.util.sort.ListUtils;
import com.hzgc.util.sort.SortParam;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public class ClusteringSearchServiceImpl {
    private static Logger LOG = Logger.getLogger(ClusteringSearchServiceImpl.class);

    /**
     * 查询聚类信息
     *
     * @param time      聚类时间
     * @param start     返回数据下标开始符号
     * @param limit     行数
     * @param sortParam 排序参数（默认按出现次数排序）
     * @return 聚类列表
     */
    List<ClusteringAttribute> clusteringSearch(String time, int start, int limit, String sortParam) {
        Table clusteringInfoTable = HBaseHelper.getTable(ClusteringTable.TABLE_ClUSTERINGINFO);
        Get get = new Get(Bytes.toBytes(time));
        List<ClusteringAttribute> clusteringList = new ArrayList<>();
        try {
            Result result = clusteringInfoTable.get(get);
            clusteringList = (List<ClusteringAttribute>) ObjectUtil.byteToObject(result.getValue(ClusteringTable.ClUSTERINGINFO_COLUMNFAMILY, ClusteringTable.ClUSTERINGINFO_COLUMN_DATA));
            if (sortParam != null && sortParam.length() > 0) {
                SortParam sortParams = ListUtils.getOrderStringBySort(sortParam);
                ListUtils.sort(clusteringList, sortParams.getSortNameArr(), sortParams.getIsAscArr());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return clusteringList;
    }

    /**
     * 查询单个聚类详细信息(告警记录)
     *
     * @param clusterId 聚类ID
     * @param time      聚类时间
     * @param start     分页查询开始行
     * @param limit     查询条数
     * @param sortParam 排序参数（默认时间先后排序）
     * @return 返回该类下面所以告警信息
     */
    List<AlarmInfo> detailClusteringSearch(String clusterId, String time, int start, int limit, String sortParam) {
        Table clusteringInfoTable = HBaseHelper.getTable(ClusteringTable.TABLE_DETAILINFO);
        Get get = new Get(Bytes.toBytes(time + "-" + clusterId));
        List<AlarmInfo> alarmInfoList = new ArrayList<>();
        try {
            Result result = clusteringInfoTable.get(get);
            alarmInfoList = (List<AlarmInfo>) ObjectUtil.byteToObject(result.getValue(ClusteringTable.ClUSTERINGINFO_COLUMNFAMILY, ClusteringTable.ClUSTERINGINFO_COLUMN_DATA));
            if (sortParam != null && sortParam.length() > 0) {
                SortParam sortParams = ListUtils.getOrderStringBySort(sortParam);
                ListUtils.sort(alarmInfoList, sortParams.getSortNameArr(), sortParams.getIsAscArr());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return alarmInfoList;
    }

    /**
     * 查询单个聚类详细信息(告警ID)
     *
     * @param clusterId 聚类ID
     * @param time      聚类时间
     * @param start     分页查询开始行
     * @param limit     查询条数
     * @param sortParam 排序参数（默认时间先后排序）
     * @return 返回该类下面所以告警信息
     */
    List<Integer> detailClusteringSearch_v1(String clusterId, String time, int start, int limit, String sortParam) {
        Table clusteringInfoTable = HBaseHelper.getTable(ClusteringTable.TABLE_DETAILINFO);
        Get get = new Get(Bytes.toBytes(time + "-" + clusterId));
        List<Integer> alarmInfoList = new ArrayList<>();
        try {
            Result result = clusteringInfoTable.get(get);
            alarmInfoList = (List<Integer>) ObjectUtil.byteToObject(result.getValue(ClusteringTable.ClUSTERINGINFO_COLUMNFAMILY, ClusteringTable.ClUSTERINGINFO_COLUMN_DATA));
            if (sortParam != null && sortParam.length() > 0) {
                SortParam sortParams = ListUtils.getOrderStringBySort(sortParam);
                ListUtils.sort(alarmInfoList, sortParams.getSortNameArr(), sortParams.getIsAscArr());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return alarmInfoList;
    }
}