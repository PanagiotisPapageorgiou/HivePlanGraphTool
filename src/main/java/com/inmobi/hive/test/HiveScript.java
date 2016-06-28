package com.inmobi.hive.test;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.text.StrSubstitutor;

import com.google.common.collect.Lists;

/*
 * This class is used to model a Hive QL script.  It will allow for parameters
 * to be substituted using the Hive convention of ${VAR_NAME}, where you would 
 * ${VAR_NAME} in your script, and then create a hashmap with VAR_NAME as the 
 * key and the substitution value as the data.  
 * 
 * In addition, you may also exclude entire lines in your script by placing 
 * the entire line in the excludes list.  
 */
public class HiveScript {

    private String scriptFile;
    private Map<String, String> params;
    private List<String> excludes;
    private StrSubstitutor substitutor;

    public HiveScript(String scriptFile, Map<String, String> params, List<String> excludes) {
        this.scriptFile = scriptFile;
        this.params = params;
        this.excludes = excludes;
        if (this.params != null) {
            substitutor = new StrSubstitutor(this.params);
        }
    }

    public List<String> getStatements() {
        List<String> commands = Lists.newArrayList();
        BufferedReader in = null;
        try {
            in = new BufferedReader(new FileReader(scriptFile));
            String line;
            StringBuilder command = new StringBuilder();
            while ((line = in.readLine()) != null) {
                if (skippableLine(line) || excludeLine(line)) {
                    continue;
                }
                if (line.endsWith(";")) {
                    command.append(replaceParams(line.replace(";", "")));
                    commands.add(command.toString());
                    command = new StringBuilder();
                }
                else {
                    command.append(replaceParams(line));
                    //need to make sure there is a space between lines
                    command.append(" ");
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        finally {
            if (in != null) {
                try {
                    in.close();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        return commands;
    }

    private boolean excludeLine(String line) {
        if (excludes == null) {
            return false;
        }
        for (String excludeLine : excludes) {
            if (line.equals(excludeLine)) {
                return true;
            }
        }
        return false;
    }

    private boolean skippableLine(String line) {
        if (line.isEmpty() || line.startsWith("--")) {
            return true;
        }
        return false;
    }

    private String replaceParams(String line) {
        if (substitutor == null) {
            return line;
        }
        return substitutor.replace(line);
    }

}
