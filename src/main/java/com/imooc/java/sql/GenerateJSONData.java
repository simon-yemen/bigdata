package com.imooc.java.sql;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

/**
 * 生成测试数据
 * Created by xuwei
 */
public class GenerateJSONData {
    public static void main(String[] args) throws Exception{
        //自适应调整Shuffle分区数量-生成测试数据
        //generateCoalescePartitionData();
        //动态调整Join策略-生成测试数据
        //generateJoinData();
        //动态优化倾斜的Join-生成测试数据
        generateSkewJoin();
    }
    /**
     * 3:动态优化倾斜的Join
     */
    private static void generateSkewJoin() throws IOException {
        //表t1
        String fileName = "D:\\spark_json_skew_t1.dat";
        System.out.println("start: 开始生成文件->" + fileName);
        BufferedWriter bfw = new BufferedWriter(new FileWriter(fileName));
        int num = 0;
        while (num < 30000000) {
            bfw.write("{\"id\":\"100" + num + "\",\"score\":" + (45 + new Random().nextInt(50)) + "}");
            bfw.newLine();
            num++;
            if (num % 10000 == 0) {
                bfw.flush();
            }
        }
        bfw.flush();

        //表t2
        fileName = "D:\\spark_json_skew_t2.dat";
        System.out.println("start: 开始生成文件->" + fileName);
        bfw = new BufferedWriter(new FileWriter(fileName));
        num = 0;
        while (num < 10000) {
            bfw.write("{\"id\":\"100" + num + "\",\"name\":\"zs" + num + "\",\"city\":\"bj\"}");
            bfw.newLine();
            num++;
            if (num % 10000 == 0) {
                bfw.flush();
            }
        }

        while (num < 30000000) {
            bfw.write("{\"id\":\"100\",\"name\":\"zs" + num + "\",\"city\":\"sh\"}");
            bfw.newLine();
            num++;
            if (num % 10000 == 0) {
                bfw.flush();
            }
        }
        bfw.flush();
        bfw.close();
    }

    /**
     * 2:动态调整Join策略
     */
    private static void generateJoinData() throws IOException {
        //表t1
        String fileName = "D:\\spark_json_t1.dat";
        System.out.println("start: 开始生成文件->"+fileName);
        BufferedWriter bfw = new BufferedWriter(new FileWriter(fileName));
        int num = 0;
        while(num<3000000){
            bfw.write("{\"id\":\"100"+num+"\",\"score\":"+(45+new Random().nextInt(50))+"}");
            bfw.newLine();
            num ++;
            if(num%10000==0){
                bfw.flush();
            }
        }
        bfw.flush();

        //表t2
        fileName = "D:\\spark_json_t2.dat";
        System.out.println("start: 开始生成文件->"+fileName);
        bfw = new BufferedWriter(new FileWriter(fileName));
        num = 0;
        while(num<10000){
            bfw.write("{\"id\":\"100"+num+"\",\"name\":\"zs"+num+"\",\"city\":\"bj\"}");
            bfw.newLine();
            num ++;
            if(num%10000==0){
                bfw.flush();
            }
        }

        while(num<3000000){
            bfw.write("{\"id\":\"100"+num+"\",\"name\":\"zs"+num+"\",\"city\":\"sh\"}");
            bfw.newLine();
            num ++;
            if(num%10000==0){
                bfw.flush();
            }
        }
        bfw.flush();
        bfw.close();
    }

    /**
     * 1:自适应调整Shuffle分区数量
     */
    private static void generateCoalescePartitionData() throws IOException {
        String fileName = "D:\\spark_json_1.dat";
        System.out.println("start: 开始生成文件->"+fileName);
        BufferedWriter bfw = new BufferedWriter(new FileWriter(fileName));
        int num = 0;
        //area = A_US
        while(num<3000000){
            bfw.write("{\"id\":\"14943445328940974601"+new Random().nextInt(1000)+"\",\"uid\":\"840717325115457536\",\"lat\":\"53.530598\",\"lnt\":\"-2.5620373\",\"hots\":0,\"title\":\"0\",\"status\":\"1\",\"topicId\":\"0\",\"end_time\":\"1494344570\",\"watch_num\":"+new Random().nextInt(10000) +",\"share_num\":\"1\",\"replay_url\":null,\"replay_num\":0,\"start_time\":\"1494344544\",\"timestamp\":1494344571,\"area\":\"A_US\"}");
            bfw.newLine();
            num ++;
            if(num%10000==0){
                bfw.flush();
            }
        }

        //flag = A_ID
        num = 0;
        while(num<1000){
            bfw.write("{\"id\":\"14943445328940974601"+new Random().nextInt(1000)+"\",\"uid\":\"840717325115457536\",\"lat\":\"53.530598\",\"lnt\":\"-2.5620373\",\"hots\":0,\"title\":\"0\",\"status\":\"1\",\"topicId\":\"0\",\"end_time\":\"1494344570\",\"watch_num\":"+new Random().nextInt(10000) +",\"share_num\":\"1\",\"replay_url\":null,\"replay_num\":0,\"start_time\":\"1494344544\",\"timestamp\":1494344571,\"area\":\"A_ID\"}");
            bfw.newLine();
            num ++;
            if(num%10000==0){
                bfw.flush();
            }
        }

        //flag = A_IN
        num = 0;
        while(num<2000){
            bfw.write("{\"id\":\"14943445328940974601"+new Random().nextInt(1000)+"\",\"uid\":\"840717325115457536\",\"lat\":\"53.530598\",\"lnt\":\"-2.5620373\",\"hots\":0,\"title\":\"0\",\"status\":\"1\",\"topicId\":\"0\",\"end_time\":\"1494344570\",\"watch_num\":"+new Random().nextInt(10000) +",\"share_num\":\"1\",\"replay_url\":null,\"replay_num\":0,\"start_time\":\"1494344544\",\"timestamp\":1494344571,\"area\":\"A_IN\"}");
            bfw.newLine();
            num ++;
            if(num%10000==0){
                bfw.flush();
            }
        }

        //flag = A_KP
        num = 0;
        while(num<3000){
            bfw.write("{\"id\":\"14943445328940974601"+new Random().nextInt(1000)+"\",\"uid\":\"840717325115457536\",\"lat\":\"53.530598\",\"lnt\":\"-2.5620373\",\"hots\":0,\"title\":\"0\",\"status\":\"1\",\"topicId\":\"0\",\"end_time\":\"1494344570\",\"watch_num\":"+new Random().nextInt(10000) +",\"share_num\":\"1\",\"replay_url\":null,\"replay_num\":0,\"start_time\":\"1494344544\",\"timestamp\":1494344571,\"area\":\"A_KP\"}");
            bfw.newLine();
            num ++;
            if(num%10000==0){
                bfw.flush();
            }
        }

        //flag = A_JP
        num = 0;
        while(num<2800000){
            bfw.write("{\"id\":\"14943445328940974601"+new Random().nextInt(1000)+"\",\"uid\":\"840717325115457536\",\"lat\":\"53.530598\",\"lnt\":\"-2.5620373\",\"hots\":0,\"title\":\"0\",\"status\":\"1\",\"topicId\":\"0\",\"end_time\":\"1494344570\",\"watch_num\":"+new Random().nextInt(10000) +",\"share_num\":\"1\",\"replay_url\":null,\"replay_num\":0,\"start_time\":\"1494344544\",\"timestamp\":1494344571,\"area\":\"A_JP\"}");
            bfw.newLine();
            num ++;
            if(num%10000==0){
                bfw.flush();
            }
        }
        bfw.flush();
        bfw.close();
        System.out.println("end: 文件已生成");
    }
}
