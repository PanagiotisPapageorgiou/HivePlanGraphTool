package madgik.exareme.master.engine;

import madgik.exareme.master.app.cluster.ExaremeCluster;
import madgik.exareme.master.app.cluster.ExaremeClusterFactory;
import madgik.exareme.master.gateway.ExaremeGateway;
import madgik.exareme.master.gateway.ExaremeGatewayFactory;
import madgik.exareme.utils.embedded.process.MadisProcess;
import madgik.exareme.utils.embedded.process.QueryResultStream;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.rmi.RemoteException;

/**
 * Created by panos on 26/10/2016.
 */
public class ExperimentThread implements Runnable {

    int numberOfNodes;
    int inputSize;
    String targetQuery = null;

    public ExperimentThread(int size, int nodes, String target){
        inputSize = size;
        numberOfNodes = nodes;
        targetQuery = target;
    }

    private static void execAndPrintResults(String query, MadisProcess proc) throws Exception {

        System.out.println("execAndPrintResults: Query = "+query);
        QueryResultStream stream = proc.execQuery(query);
        System.out.println(stream.getSchema());
        String record = stream.getNextRecord();
        while (record != null) {
            System.out.println("execAndPrintResults: "+record);
            record = stream.getNextRecord();
        }

    }

    public void runMadisExperiment(String query, String queryTag, PrintWriter outputFile){

        MadisProcess madisProcess = new MadisProcess("","/home/panos/exareme-dev/exareme-tools/madis/src/mterm.py");

        //Start Madis
        try {
            System.out.println("Start Madis...");
            madisProcess.start();
        } catch(java.io.IOException madisStartEx){
            System.out.println("IO Exception in madisProcess.start(): "+madisStartEx.getMessage());
            return;
        }
        catch(Exception simpleEx){
            System.out.println("Exception in madisProcess.start(): "+simpleEx.getMessage());
            return;
        }

        try {
            long startTime;
            long endTime;

            if(queryTag.contains("DROP")){
                System.out.println(queryTag);
                //startTime = System.currentTimeMillis();
                execAndPrintResults(query, madisProcess);
                //endTime = System.currentTimeMillis();
                //System.out.println("Duration: "+ (endTime - startTime));
                //outputFile.println("Time: "+(endTime - startTime));
                //outputFile.flush();
            }
            else{
                if(queryTag.equals("Q1")){
                    outputFile.println("----------------EXPERIMENT: NODE: "+numberOfNodes+" - Size: "+inputSize+" - Query: Q1");
                }
                else if(queryTag.equals("Q2")){
                    outputFile.println("----------------EXPERIMENT: NODE: "+numberOfNodes+" - Size: "+inputSize+" - Query: Q2");
                }
                else if(queryTag.equals("Q3")){
                    outputFile.println("----------------EXPERIMENT: NODE: "+numberOfNodes+" - Size: "+inputSize+" - Query: Q3");
                }
                else{
                    outputFile.println("----------------EXPERIMENT: NODE: "+numberOfNodes+" - Size: "+inputSize+" - Query: Q4");
                }

                System.out.println(queryTag);
                startTime = System.currentTimeMillis();
                execAndPrintResults(query, madisProcess);
                endTime = System.currentTimeMillis();
                System.out.println("Duration: "+ (endTime - startTime));
                outputFile.println("Time: "+(endTime - startTime));
                outputFile.flush();
            }

            try {
                System.out.println("Stop Madis...");
                madisProcess.stop();
            } catch(java.io.IOException ioEx){
                System.out.println("IO Exception in madisProcess.stop(): "+ioEx.getMessage());
                System.exit(0);
            }
            catch(java.lang.InterruptedException interEx){
                System.out.println("java.lang.interruptedEx madisProcess.stop(): "+interEx.getMessage());
                System.exit(0);
            }
            catch(Exception stopEx){
                System.out.println("Exception in madisProcess.stop(): "+stopEx.getMessage());
                System.exit(0);
            }

        } catch(java.lang.Exception q1){

            System.out.println("Exception in "+queryTag+" -  "+q1.getMessage());
            try {
                System.out.println("Stop Madis...");
                madisProcess.stop();
            } catch(java.io.IOException ioEx){
                System.out.println("IO Exception in madisProcess.stop(): "+ioEx.getMessage());
                System.exit(0);
            }
            catch(java.lang.InterruptedException interEx){
                System.out.println("java.lang.interruptedEx madisProcess.stop(): "+interEx.getMessage());
                System.exit(0);
            }
            catch(Exception stopEx){
                System.out.println("Exception in madisProcess.stop(): "+stopEx.getMessage());
                System.exit(0);
            }
            return;
        }

    }

    public void run(){

        Logger.getRootLogger().setLevel(Level.ALL);

        String startPart = "exaquery 'db:/home/panos/";

        if(inputSize == 1){
            startPart = startPart + "exareme1GB/tpcds_db.db/' ";
        }
        else if(inputSize == 10){
            startPart = startPart + "exareme10GB/tpcds_db.db/' ";
        }
        else if(inputSize == 50){
            startPart = startPart + "exareme50GB/tpcds_db.db/' ";
        }
        else{
            startPart = startPart + "exareme100GB/tpcds_db.db/' ";
        }

        String query1 = startPart + " distributed drop table queryExperiment1 ;";

        String query2 = startPart + " distributed drop table queryExperiment2 ;";

        String query3 = startPart + " distributed drop table queryExperiment3 ;";

        String query4 = startPart + " distributed drop table queryExperiment4 ;";

        String expQuery1 = startPart + " distributed create temporary table queryExperiment1 as select  i_item_id, avg(cs_quantity) as agg1, avg(cs_list_price) as agg2, avg(cs_coupon_amt) as agg3, avg(cs_sales_price) as agg4 from catalog_sales, customer_demographics, date_dim, item, promotion where catalog_sales.cs_sold_date_sk = date_dim.d_date_sk and catalog_sales.cs_item_sk = item.i_item_sk and catalog_sales.cs_bill_cdemo_sk = customer_demographics.cd_demo_sk and catalog_sales.cs_promo_sk = promotion.p_promo_sk and catalog_sales.cs_promo_sk = promotion.p_promo_sk and catalog_sales.cs_promo_sk = promotion.p_promo_sk and cd_gender = 'F' and cd_marital_status = 'W' and cd_education_status = 'Primary' and (p_channel_email = 'N' or p_channel_event = 'N') and d_year = 1998 group by i_item_id order by i_item_id limit 100;";

        String expQuery2 = startPart + " distributed create temporary table queryExperiment2 as select ssales.c_last_name,ssales.c_first_name,ssales.s_store_name,sum(ssales.netpaid) paid,avg(ssales.netpaid) as avgpaid from (select c_last_name,c_first_name,s_store_name,ca_state,s_state,i_color,i_current_price,i_manager_id,i_units,i_size,sum(ss_sales_price) netpaid from store_sales,store_returns,store,item,customer,customer_address where ss_ticket_number = sr_ticket_number and ss_item_sk = sr_item_sk and ss_customer_sk = c_customer_sk and ss_item_sk = i_item_sk and ss_store_sk = s_store_sk and c_birth_country = upper(ca_country) and s_zip = ca_zip and s_market_id=7 group by c_last_name ,c_first_name ,s_store_name,ca_state,s_state,i_color,i_current_price,i_manager_id,i_units,i_size) ssales where ssales.i_color = 'orchid' group by ssales.c_last_name,ssales.c_first_name,ssales.s_store_name having sum(ssales.netpaid) > 0.05 * avg(ssales.netpaid);";

        String expQuery3 = startPart + " distributed create temporary table queryExperiment3 as select i_brand_id brand_id, i_brand brand,t_hour,t_minute, sum(ext_price) as ext_price from item JOIN (select ws_ext_sales_price as ext_price, ws_sold_date_sk as sold_date_sk, ws_item_sk as sold_item_sk, ws_sold_time_sk as time_sk from web_sales,date_dim where date_dim.d_date_sk = web_sales.ws_sold_date_sk and d_moy=12 and d_year=2001 union all select cs_ext_sales_price as ext_price, cs_sold_date_sk as sold_date_sk, cs_item_sk as sold_item_sk,cs_sold_time_sk as time_sk from catalog_sales,date_dim where date_dim.d_date_sk = catalog_sales.cs_sold_date_sk and d_moy=12 and d_year=2001 union all select ss_ext_sales_price as ext_price, ss_sold_date_sk as sold_date_sk, ss_item_sk as sold_item_sk, ss_sold_time_sk as time_sk from store_sales,date_dim where date_dim.d_date_sk = store_sales.ss_sold_date_sk and d_moy=12 and d_year=2001) tmp ON tmp.sold_item_sk = item.i_item_sk JOIN time_dim ON tmp.time_sk = time_dim.t_time_sk where i_manager_id=1 and (t_meal_time = 'breakfast' or t_meal_time = 'dinner') group by i_brand, i_brand_id,t_hour,t_minute order by ext_price desc, i_brand_id;";

        String expQuery4 = startPart + " distributed create temporary table queryExperiment4 as select i_item_id, ca_country, ca_state, ca_county, avg(agg1), avg(agg2), avg(agg3), avg(agg4), avg(agg5), avg(agg6), avg(agg7) from (select i_item_id, ca_country, ca_state, ca_county, cast(cs_quantity as real) as agg1, cast(cs_list_price as real) as agg2, cast(cs_coupon_amt as real) as agg3, cast(cs_sales_price as real) as agg4, cast(cs_net_profit as real) as agg5, cast(c_birth_year as real) as agg6, cast(cd_dep_count as real) as agg7 from catalog_sales, customer_demographics , customer, customer_address, date_dim, item where cs_sold_date_sk = d_date_sk and cs_item_sk = i_item_sk and cs_bill_cdemo_sk = cd_demo_sk and cs_bill_customer_sk = c_customer_sk and cd_gender = 'M' and cd_education_status = 'Secondary' and c_current_addr_sk = ca_address_sk union all select i_item_id, ca_country, ca_state, ca_county, cast(cs_quantity as real) as agg1, cast(cs_list_price as real) as agg2, cast(cs_coupon_amt as real) as agg3, cast(cs_sales_price as real) as agg4, cast(cs_net_profit as real) as agg5, cast(c_birth_year as real) as agg6, cast(cd_dep_count as real) as agg7 from catalog_sales, customer_demographics , customer, customer_address, date_dim, item where cs_sold_date_sk = d_date_sk and cs_item_sk = i_item_sk and cs_bill_cdemo_sk = cd_demo_sk and cs_bill_customer_sk = c_customer_sk and cd_gender = 'M' and cd_education_status = 'University' and c_current_addr_sk = ca_address_sk union all select i_item_id, ca_country, ca_state, ca_county, cast(cs_quantity as real) as agg1, cast(cs_list_price as real) as agg2, cast(cs_coupon_amt as real) as agg3, cast(cs_sales_price as real) as agg4, cast(cs_net_profit as real) as agg5, cast(c_birth_year as real) as agg6, cast(cd_dep_count as real) as agg7 from catalog_sales, customer_demographics , customer, customer_address, date_dim, item where cs_sold_date_sk = d_date_sk and cs_item_sk = i_item_sk and cs_bill_cdemo_sk = cd_demo_sk and cs_bill_customer_sk = c_customer_sk and cd_gender = 'F' and cd_education_status = 'Primary' and c_current_addr_sk = ca_address_sk) foo group by i_item_id, ca_country, ca_state, ca_county order by ca_country, ca_state, ca_county, i_item_id limit 100;";

        try {

            final ExaremeCluster cluster = ExaremeClusterFactory.createMiniCluster(1098, 8088, numberOfNodes);

            final ExaremeGateway gateway;

            System.out.println("------------SETUP EXPERIMENT - NODES: "+numberOfNodes+" - SIZE: "+inputSize+"--------------");

            cluster.start();
            try {
                gateway = ExaremeGatewayFactory.createHttpServer(cluster.getDBManager());
            } catch(java.lang.Exception ex){
                System.out.println("Exception while createHttpServer: " + ex.getMessage());
                return;
            }

            try {
                gateway.start();
            } catch(java.lang.Exception ex){
                System.out.println("Exception in gateway start: " + ex.getMessage());
                return;
            }

            //System.out.println("createSQLiteTableFromHiveTable:Now Running MadisProcess to fill the new table!");

            if(numberOfNodes == 1){
                if(inputSize <= 10){

                    PrintWriter outputFile = null;
                    try {
                        outputFile = new PrintWriter(new BufferedWriter(new FileWriter("/home/panos/allExaremeTimes/1NODE"+inputSize+"GB.txt", true)));
                    } catch (IOException e) {
                        System.out.println("Exception creating new PrintWriter..."+e.getMessage());
                        return;
                    }

                    for(int i = 0 ; i < 2; i++) {
                        //runMadisExperiment(query1, "DROP1", outputFile);

                        if(targetQuery == null){
                            runMadisExperiment(expQuery1, "Q1", outputFile);

                            //runMadisExperiment(query2, "DROP2", outputFile);

                            runMadisExperiment(expQuery2, "Q2", outputFile);

                            //runMadisExperiment(query3, "DROP3", outputFile);

                            runMadisExperiment(expQuery3, "Q3", outputFile);

                            //runMadisExperiment(query4, "DROP4", outputFile);

                            runMadisExperiment(expQuery4, "Q4", outputFile);
                        }
                        else{
                            if(targetQuery.equals("Q1")){
                                runMadisExperiment(expQuery1, "Q1", outputFile);
                            }
                            else if(targetQuery.equals("Q2")){
                                runMadisExperiment(expQuery2, "Q2", outputFile);
                            }
                            else if(targetQuery.equals("Q3")){
                                runMadisExperiment(expQuery3, "Q3", outputFile);
                            }
                            else if(targetQuery.equals("Q4")){
                                runMadisExperiment(expQuery4, "Q4", outputFile);
                            }

                        }

                    }

                }
            }
            else if(numberOfNodes == 2){
                PrintWriter outputFile = null;
                try {
                    outputFile = new PrintWriter(new BufferedWriter(new FileWriter("/home/panos/allExaremeTimes/2NODE"+inputSize+"GB.txt", true)));
                } catch (IOException e) {
                    System.out.println("Exception creating new PrintWriter..."+e.getMessage());
                    return;
                }
                if(inputSize <= 50){
                    if(inputSize <= 10){
                        for(int i = 0 ; i < 2; i++) {
                            if(targetQuery == null){
                                runMadisExperiment(expQuery1, "Q1", outputFile);

                                //runMadisExperiment(query2, "DROP2", outputFile);

                                runMadisExperiment(expQuery2, "Q2", outputFile);

                                //runMadisExperiment(query3, "DROP3", outputFile);

                                runMadisExperiment(expQuery3, "Q3", outputFile);

                                //runMadisExperiment(query4, "DROP4", outputFile);

                                runMadisExperiment(expQuery4, "Q4", outputFile);
                            }
                            else{
                                if(targetQuery.equals("Q1")){
                                    runMadisExperiment(expQuery1, "Q1", outputFile);
                                }
                                else if(targetQuery.equals("Q2")){
                                    runMadisExperiment(expQuery2, "Q2", outputFile);
                                }
                                else if(targetQuery.equals("Q3")){
                                    runMadisExperiment(expQuery3, "Q3", outputFile);
                                }
                                else if(targetQuery.equals("Q4")){
                                    runMadisExperiment(expQuery4, "Q4", outputFile);
                                }

                            }

                        }
                    }
                    else{
                        if(targetQuery == null){
                            runMadisExperiment(expQuery1, "Q1", outputFile);

                            //runMadisExperiment(query2, "DROP2", outputFile);

                            runMadisExperiment(expQuery2, "Q2", outputFile);

                            //runMadisExperiment(query3, "DROP3", outputFile);

                            runMadisExperiment(expQuery3, "Q3", outputFile);

                            //runMadisExperiment(query4, "DROP4", outputFile);

                            runMadisExperiment(expQuery4, "Q4", outputFile);
                        }
                        else{
                            if(targetQuery.equals("Q1")){
                                runMadisExperiment(expQuery1, "Q1", outputFile);
                            }
                            else if(targetQuery.equals("Q2")){
                                runMadisExperiment(expQuery2, "Q2", outputFile);
                            }
                            else if(targetQuery.equals("Q3")){
                                runMadisExperiment(expQuery3, "Q3", outputFile);
                            }
                            else if(targetQuery.equals("Q4")){
                                runMadisExperiment(expQuery4, "Q4", outputFile);
                            }

                        }
                    }
                }
            }
            else if(numberOfNodes == 4){
                PrintWriter outputFile = null;
                try {
                    outputFile = new PrintWriter(new BufferedWriter(new FileWriter("/home/panos/allExaremeTimes/4NODE"+inputSize+"GB.txt", true)));
                } catch (IOException e) {
                    System.out.println("Exception creating new PrintWriter..."+e.getMessage());
                    return;
                }
                if(inputSize <= 10){
                    for(int i = 0 ; i < 2; i++) {
                        if(targetQuery == null){
                            //runMadisExperiment(expQuery1, "Q1", outputFile);

                            //runMadisExperiment(query2, "DROP2", outputFile);

                            runMadisExperiment(expQuery2, "Q2", outputFile);

                            //runMadisExperiment(query3, "DROP3", outputFile);

                            //runMadisExperiment(expQuery3, "Q3", outputFile);

                            //runMadisExperiment(query4, "DROP4", outputFile);

                            runMadisExperiment(expQuery4, "Q4", outputFile);
                        }
                        else{
                            if(targetQuery.equals("Q1")){
                                runMadisExperiment(expQuery1, "Q1", outputFile);
                            }
                            else if(targetQuery.equals("Q2")){
                                runMadisExperiment(expQuery2, "Q2", outputFile);
                            }
                            else if(targetQuery.equals("Q3")){
                                runMadisExperiment(expQuery3, "Q3", outputFile);
                            }
                            else if(targetQuery.equals("Q4")){
                                runMadisExperiment(expQuery4, "Q4", outputFile);
                            }

                        }

                    }
                }
                else{
                    if(targetQuery == null){
                        runMadisExperiment(expQuery1, "Q1", outputFile);

                        //runMadisExperiment(query2, "DROP2", outputFile);

                        runMadisExperiment(expQuery2, "Q2", outputFile);

                        //runMadisExperiment(query3, "DROP3", outputFile);

                        runMadisExperiment(expQuery3, "Q3", outputFile);

                        //runMadisExperiment(query4, "DROP4", outputFile);

                        runMadisExperiment(expQuery4, "Q4", outputFile);
                    }
                    else{
                        if(targetQuery.equals("Q1")){
                            runMadisExperiment(expQuery1, "Q1", outputFile);
                        }
                        else if(targetQuery.equals("Q2")){
                            runMadisExperiment(expQuery2, "Q2", outputFile);
                        }
                        else if(targetQuery.equals("Q3")){
                            runMadisExperiment(expQuery3, "Q3", outputFile);
                        }
                        else if(targetQuery.equals("Q4")){
                            runMadisExperiment(expQuery4, "Q4", outputFile);
                        }

                    }

                }
            }


            Runtime.getRuntime().addShutdownHook(new Thread() {
                @Override public void run() {
                    gateway.stop();
                    try {
                        cluster.stop(true);
                    } catch (RemoteException e) {
                        e.printStackTrace();
                    }
                }
            });

            gateway.stop();
            try {
                cluster.stop(true);
            } catch (RemoteException e) {
                e.printStackTrace();
            }

            return;

        } catch(java.rmi.RemoteException rmiEx){
            System.out.println("Failed with RMI at start... Exception is: " + rmiEx.getMessage());
            return;
        }


    }

}
