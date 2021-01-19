package com.brezp.spark.summup.driver;



import java.io.IOException;
import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.*;

/**
 * nohup sh run.sh com.datastory.nova.etl.cli.mdlz.WeiboHistoryV2Cli -from 202012 -to 202101 > WeiboHistoryV2Cli.log &
 */
public class WeiboHistoryV2Cli implements Serializable {


    private static Logger LOG = LoggerFactory.getLogger(WeiboHistoryV2Cli.class);
    private ThreadPoolExecutor pool;
    private ReaderApi userReader;
    private WriterApi writerApi;

    private JavaSparkContext javaSparkContext;

    private static final Integer CORE_POOL_SIZE = 16;
    private static final Integer MAX_POOL_SIZE = 50;
    private static final Integer THREAD_IDLE_KEEP_TIME = 1000;
    private static final Integer TASK_QUEUE_SIZE = 5;
    private static final Integer SCROLL_SIZE = 500;

    private String fromStr = "201910";
    private String toStr = "201911";

    private static final String CLI_PARAMS_FROM = "from";
    private static final String CLI_PARAMS_TO = "to";
    private Long from;
    private Long to;


    @Override
    public Options initOptions() {
        Options options = new Options();
        options.addOption(CLI_PARAMS_FROM, true, "weibo content start time: yyyyMM");
        options.addOption(CLI_PARAMS_TO, true, "weibo content end time: yyyyMM");
        return options;
    }

    @Override
    public boolean validateOptions(CommandLine commandLine) {
        return true;
    }

    public static void main(String[] args) {
        AdvCli.initRunner(args, "WeiboHistoryCli", new WeiboHistoryV2Cli());
    }

    @Override
    public void start(CommandLine commandLine) {
        this.initCmdParam(commandLine);
        this.execute();
    }

    private void initSparkTaskConfig() {
        SparkYarnConf sparkYarnConf = new SparkYarnConf("yarn-client");
        sparkYarnConf.setAppName("亿滋-微博本月用户帖子词云聚合" + System.currentTimeMillis())
                .setYarnQueue("ds.support")
                .setHdfsYarnJar()
                .setDriverMemory("4g")
                .setExecutorMemory("4g")
                .setNumExectors(12)
                .setExecutorCores(8)
                .setJars("hdfs:///projects/datastory/nova/elasticsearch-spark-13_2.10-5.6.8.jar")
                .setClassAndJar(WeiboHistoryV2Cli.class)
                .setClassAndJar(WeiboHistoryTaskV2.class);
        // 如果是local模型,则获取DemoSubmit的SparkConf,否则new SparkConf
        SparkConf sparkConf = sparkYarnConf.getSparkConf();
        // String esHost = NovaConstants.ES_HOST.split(",")[0];
        String esNode = esHost.split(":")[0];
        String esPort = esHost.split(":")[1];
        sparkConf.set("es.nodes", esNode);
        sparkConf.set("es.port", esPort);
        sparkConf.set("spark.driver.maxResultSize","4g");
        sparkConf.set("es.scroll.size", "1000");
        javaSparkContext = new JavaSparkContext(sparkConf);
    }


    @Override
    public void execute() {
        long startTime;
        long endTime;
        try {
            Date startDate = DateUtils.parseDate(fromStr, "yyyyMM");
            startTime = startDate.getTime();
            Date endDate = DateUtils.parseDate(toStr, "yyyyMM");
            endTime = endDate.getTime();
            SimpleDateFormat sdf = new SimpleDateFormat("yyyyMM");
            from = sdf.parse(fromStr).getTime();
            to = sdf.parse(toStr).getTime();
        } catch (ParseException e) {
            throw new RuntimeException(e.getMessage());
        }
        this.initSparkTaskConfig();
        // 查询符合条件的用户
        BoolQueryBuilder queryBuilder = new BoolQueryBuilder();
        queryBuilder.must(QueryBuilders.rangeQuery("publish_time").gte(startTime).lt(endTime));

        try {
            long totalNumber = userReader.count(queryBuilder);
            LOG.info("total user is: {}", totalNumber);
            if (totalNumber < 1) {
                LOG.error("user is empty.");
                return;
            }
            long requestTimes = totalNumber / SCROLL_SIZE;
            if (totalNumber % SCROLL_SIZE > 0) {
                requestTimes++;
            }
            String[] includeSources = new String[]{"uid", "Titan_concept_name_and_val", "Titan_concept_name", "Titan_category"};
            EsReaderResult readerResult = userReader.scroll(queryBuilder, SCROLL_SIZE, includeSources);
            int i = 0;
            LinkedList<Future> futureLinkedList = new LinkedList<>();
            //test
            //requestTimes = 1;
            List<Map> userMapList = new ArrayList<>();
            while (readerResult != null && !readerResult.isEnd() && i < requestTimes) {
                LOG.info("基于用户索引查询UserMap，查询第 {} 次,共 {} 次", i, requestTimes);
                Map<String, Map<String, Object>> userMap = new HashMap<>();
                i++;
                SearchHit[] searchHits = readerResult.getDatas();
                for (SearchHit searchHit : searchHits) {
                    Map<String, Object> source = searchHit.getSource();
                    userMap.put(String.valueOf(source.get("uid")), source);
                }
                userMapList.add(userMap);
                readerResult = userReader.scroll(readerResult.getScrollId());
            }
            LOG.info("基于UserMap提交Spark查询全量库");
            List<WeiboContentEntity> weiboContentEntitiesSet = this.banyanQueryWeiboContentBySpark(userMapList);
            LOG.info("查询完成，weiboContentEntitiesSet size：{}", weiboContentEntitiesSet.size());
            List<WeiboContentAggsEntity> weiboContentAggsEntities = this.aggsBySpark(weiboContentEntitiesSet);
            LOG.info("聚合完成 开始写入ES：{}", weiboContentAggsEntities.size());
            weiboContentAggsEntities.forEach(t -> {
                try {
                    writerApi.addDoc(BeanUtil.beanToMap(t));
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
            writerApi.flush();
            LOG.info("写入ES中：{}", weiboContentAggsEntities.size());
            Thread.sleep(20000);
            LOG.info("写入完成：{}", weiboContentAggsEntities.size());
        } catch (Exception e) {
            e.printStackTrace();
            LOG.error(e.getMessage());
        } finally {
            userReader.close();
        }
        LOG.info("program done!");
        System.exit(0);
    }


    private List<WeiboContentEntity> banyanQueryWeiboContentBySpark(List<Map> userMapList) {
        JavaRDD<Map> userMapRDD = javaSparkContext.parallelize(userMapList);
        Long from1 = from;
        Long to1 = to;
        for (Map map : userMapList) {
            LOG.info("banyanQueryWeiboContentBySpark: userMap ,size :{}", map.entrySet().size());
        }
        JavaRDD<List<WeiboContentEntity>> wbContentEntityListRDD = userMapRDD.map(map -> {
            WeiboHistoryTaskV2 weiboHistoryTaskV2 = new WeiboHistoryTaskV2(map, from1, to1);
            List<WeiboContentEntity> call = weiboHistoryTaskV2.call();
            return call;
        });
        List<WeiboContentEntity> result = wbContentEntityListRDD.reduce((set1, set2) -> {
            List<WeiboContentEntity> setSum = new ArrayList<>();
            setSum.addAll(set1);
            setSum.addAll(set2);
            return setSum;
        });
        return result;
    }

    private List<WeiboContentAggsEntity> aggsBySpark(List<WeiboContentEntity> weiboContentEntitiesSet) {

        JavaRDD<WeiboContentEntity> wbContentRDD = javaSparkContext.parallelize(weiboContentEntitiesSet);
        //品类
        Map<String, Long> categoryCountMap = wbContentRDD.flatMap((weiboContentEntity) -> {
            List<String> result = new ArrayList<>();
            if (weiboContentEntity.getTitanCategory() != null && weiboContentEntity.getTitanConceptNameAndVal() != null) {
                weiboContentEntity.getTitanCategory().forEach(category -> {
                    weiboContentEntity.getTitanConceptNameAndVal().forEach(conceptNameAndVal -> {
                        //取品类组成key
                        if (weiboContentEntity.getSelfContentCategory() != null) {
                            weiboContentEntity.getSelfContentCategory().forEach(followCategory -> {
                                String key = category + "#" + conceptNameAndVal + "#" + followCategory;
                                result.add(key);
                            });
                        }
                    });
                });
            }
            return result;
        }).countByValue();
        LOG.info("聚合完成，categoryCountMap size：{}", categoryCountMap.size());
        //品牌
        Map<String, Long> brandCountMap = wbContentRDD.flatMap((weiboContentEntity) -> {
            List<String> result = new ArrayList<>();
            if (weiboContentEntity.getTitanCategory() != null && weiboContentEntity.getTitanConceptNameAndVal() != null) {
                weiboContentEntity.getTitanCategory().forEach(category -> {
                    weiboContentEntity.getTitanConceptNameAndVal().forEach(conceptNameAndVal -> {
                        //取品类组成key
                        if (weiboContentEntity.getSelfContentBrand() != null) {
                            weiboContentEntity.getSelfContentBrand().forEach(brand -> {
                                String key = category + "#" + conceptNameAndVal + "#" + brand;
                                result.add(key);
                            });
                        }
                    });
                });
            }
            return result;
        }).countByValue();
        LOG.info("聚合完成，brandCountMap size：{}", brandCountMap.size());
        List<WeiboContentAggsEntity> resultList = new ArrayList<>();
        resultList.addAll(this.genWeiboContentAggsEntityResult(categoryCountMap, "category"));
        resultList.addAll(this.genWeiboContentAggsEntityResult(brandCountMap, "brand"));
        return resultList;
    }


    private List<WeiboContentAggsEntity> genWeiboContentAggsEntityResult(Map<String, Long> countMap, String type) {
        List<WeiboContentAggsEntity> result = new ArrayList<>(countMap.size());
        countMap.forEach((key, val) -> {
            //LOG.info("categoryCountMap ： key val  : {} , {} ", key, val);
            String category = key.split("#")[0];
            String conceptNameAndVal = key.split("#")[1];
            String wordName = key.split("#")[2];
            String id = Md5Util.md5(type + category + conceptNameAndVal + wordName + fromStr);
            result.add(new WeiboContentAggsEntity(id, key, category, conceptNameAndVal, from, fromStr, type, wordName, val));
        });
        return result;
    }

    private void initCmdParam(CommandLine commandLine) {
        LOG.info("cluster:" + ConfigUtil.ES_CLUSTER);
        EsClient client = null;
        try {
            client = new EsClient.Builder()
                    .setCluster(ConfigUtil.ES_CLUSTER)
                    .setEsHosts(new String[]{ConfigUtil.ES_HOSTS})
                    .build();
        } catch (Exception e) {
            e.printStackTrace();
        }

        userReader = new ReaderApi(client, ConfigUtil.NOVA_USER_INDEX, ConfigUtil.NOVA_USER_TYPE);
        this.writerApi = new WriterApi(client, "ds-mdlz-weibo-content-v10", "content");

        BlockingQueue<Runnable> queue = new ArrayBlockingQueue<>(TASK_QUEUE_SIZE);
        pool = new ThreadPoolExecutor(CORE_POOL_SIZE, MAX_POOL_SIZE, THREAD_IDLE_KEEP_TIME, TimeUnit.SECONDS, queue, new ThreadFactoryBuilder().setNameFormat("weibo-history-%d").build());

        if (commandLine.hasOption(CLI_PARAMS_FROM)) {
            fromStr = commandLine.getOptionValue(CLI_PARAMS_FROM);
        }

        if (commandLine.hasOption(CLI_PARAMS_TO)) {
            toStr = commandLine.getOptionValue(CLI_PARAMS_TO);
        }
    }


}


