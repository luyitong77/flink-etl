package com.bdilab.flinketl.utils.kafka;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

/**
 * @author fanlei
 */

public class KafkaUtils {

    public static String getFilePath(String confPath,String username,String password) {
        String f = "KafkaClient {\n" +
                "  org.apache.kafka.common.security.scram.ScramLoginModule required\n" +
                "  username="+username+"\n" +
                "  password="+password+";\n" +
                "};";
        File file = new File(confPath+username+"_"+password);
        FileWriter fw = null;
        BufferedWriter bw = null;
        try {
            if(!file.exists()){
                file.createNewFile();
                fw = new FileWriter(file);
                bw = new BufferedWriter(fw);
                bw.write(f);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if(bw != null){
                try {
                    bw.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if(fw != null){
                try {
                    fw.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return file.getAbsolutePath();
    }

}
