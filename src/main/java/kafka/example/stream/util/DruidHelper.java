package kafka.example.stream.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.metamx.tranquility.tranquilizer.Tranquilizer;
import kafka.example.stream.druid.DruidBeamFactory;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.params.CoreConnectionPNames;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.util.Map;

public class DruidHelper {

    private static final String druidTaskFailed = "FAILED";
    private static final String druidTaskRunning = "RUNNING";
    private static final String druidTaskSuccess = "SUCCESS";
    private static final int oneSleep = 3000;
    private static final int maxSleep = 30000;
    private static final int CONNECTION_TIMEOUT = 65000;
    private static final int SO_TIMEOUT = 65000;

    public static Tranquilizer<Map<String, Object>> getTranquilizer(
            DruidBeamFactory beamFactory,
            String dataSourceName) {

        Tranquilizer<Map<String, Object>> tranquilizer = Tranquilizer.builder()
                .maxBatchSize(Configurations.getInstance().druidTranquilityMaxBatchSize)
                .maxPendingBatches(Configurations.getInstance().druidTranquilityMaxPendingBatch)
                .lingerMillis(Configurations.getInstance().druidTranquilityLingerMs)
                .blockOnFull(true)
                .build(beamFactory.makeBeam(dataSourceName));
        tranquilizer.start();

        return tranquilizer;
    }

    public static boolean submitTask(String jsonStr, boolean logDetail) {
        boolean re = true;
        HttpClient httpClient = null;
        try {
            httpClient = new DefaultHttpClient();
            httpClient.getParams().setParameter(CoreConnectionPNames.CONNECTION_TIMEOUT, CONNECTION_TIMEOUT);
            httpClient.getParams().setParameter(CoreConnectionPNames.SO_TIMEOUT, SO_TIMEOUT);

            if (logDetail) {
                LogHelper.info("JsonStr:" + jsonStr);
            }
            HttpPost post = new HttpPost(Configurations.getInstance().druidIndexServerUrl);
            post.setHeader("Content-type", "application/json");
            StringEntity entity = new StringEntity(jsonStr);
            entity.setContentEncoding("UTF-8");
            entity.setContentType("application/json");
            post.setEntity(entity);

            HttpResponse response = null;

            int statusCode = HttpStatus.SC_OK;
            String result = "";
            try {
                response = httpClient.execute(post);
                statusCode = response.getStatusLine().getStatusCode();
                result = EntityUtils.toString(response.getEntity());
                if (logDetail) {
                    LogHelper.info("response result::" + result);
                    LogHelper.info("response Code::" + statusCode);
                }
            } catch (Exception ignore) {
            } finally {
                try {
                    post.releaseConnection();
                } catch (Exception ignore) {
                }
            }

            JSONObject objStatus = null;
            if (statusCode != HttpStatus.SC_OK) {
                LogHelper.info("Failed to post task to Druid Indexing Server: response " + statusCode);
            } else {
                JSONObject obj;
                try {
                    obj = JSON.parseObject(result);
                    String taskId = (String) obj.get("task");

                    if (logDetail) {
                        LogHelper.info("Submit ingestion Task: " + taskId + ", url: " + Configurations.getInstance().druidIndexServerUrl);
                    }
                    //query task status
                    String taskQueryStatus = Configurations.getInstance().druidIndexServerUrl + "/" + taskId + "/status";
                    String status = "";
                    int totalSleep = 0;
                    do {
                        try {
                            Thread.sleep(oneSleep);
                            totalSleep += oneSleep;
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }

                        HttpGet get = new HttpGet(taskQueryStatus);
                        HttpResponse statusRes = null;
                        try {
                            statusRes = httpClient.execute(get);
                        } catch (Exception ignore) {
                        } finally {
                            get.releaseConnection();
                        }
                        try {
                            objStatus = JSON.parseObject(EntityUtils.toString(statusRes.getEntity()));
                            status = (String) (((JSONObject) (objStatus.get("status"))).get("status"));
                        } catch (Exception e) {
                            e.printStackTrace();
                            LogHelper.info("Task: " + taskId + " exception, when query task status" + e.getMessage());
                        }
                        if (totalSleep >= maxSleep) {
                            LogHelper.info("Task: " + taskId + " has not finished in " + maxSleep + " milliseconds, no wait any more.");
                            break;
                        }
                    } while (status.equals(druidTaskRunning));
                    if (logDetail) {
                        LogHelper.info("objStatus = " + objStatus);
                        LogHelper.info("status = " + status);
                    }
                    re = status.equals(druidTaskSuccess);
                } catch (Exception e) {
                    e.printStackTrace();
                    re = false;
                    LogHelper.info("task exception: " + e.getMessage());
                }
            }

        } catch (IOException e) {
            e.printStackTrace();
            re = false;
            LogHelper.info("task exception: " + e.getMessage());
        } finally {
            httpClient = null;
        }

        return re;
    }

    public static String submitQuery(String jsonStr, int retryTimes, boolean logResult) {
        int retrySleepMs = 3000;
        String re = "";
        int tryTimes = 0;
        while ("".equals(re)) {
            re = DruidHelper.submitQuery(jsonStr, logResult);
            if (tryTimes++ > retryTimes) {
                break;
            }
            if ("".equals(re)) {
                LogHelper.info("query druid get no result from druid, sleep awhile will query again, try times: " + tryTimes);
                try {
                    Thread.sleep(retrySleepMs);
                } catch (Exception ignore) {
                }
                retrySleepMs += 2000;
            }
        }
        return re;
    }

    public static String submitQuery(String jsonStr, boolean logResult) {
        String url = Configurations.getInstance().druidQueryUrl;
        HttpClient httpClient = null;
        String result = "";
        try {
            httpClient = new DefaultHttpClient();
            httpClient.getParams().setParameter(CoreConnectionPNames.CONNECTION_TIMEOUT, CONNECTION_TIMEOUT);
            httpClient.getParams().setParameter(CoreConnectionPNames.SO_TIMEOUT, SO_TIMEOUT);

            if (logResult) {
                LogHelper.info("JsonStr:" + jsonStr);
            }
            HttpPost post = new HttpPost(url);
            post.setHeader("Content-type", "application/json");
            StringEntity entity = new StringEntity(jsonStr);
            entity.setContentEncoding("UTF-8");
            entity.setContentType("application/json");
            post.setEntity(entity);

            HttpResponse response = null;
            try {
                response = httpClient.execute(post);
                result = EntityUtils.toString(response.getEntity());
                if (logResult) {
                    LogHelper.info("response result::" + result);
                }
            } catch (Exception ignore) {
            } finally {
                try {
                    post.releaseConnection();
                } catch (Exception ignore) {
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
            LogHelper.info("[task exception: " + e.getMessage());
        } finally {
            httpClient = null;
        }

        return result;
    }
}