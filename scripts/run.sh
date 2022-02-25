spark-submit --master yarn --deploy-mode cluster --conf spark.driver.cores=4 --conf spark.driver.memory=8g --conf spark.executor.cores=1 --conf spark.executor.memory=2600M --num-executors 8 --class com.github.fernandobontorin.tcc.EstabelecimentoJob s3://fernando-bontorin-deploys/book-estabelecimentos-assembly-0.2.3.jar --inputs s3a://fernando-bontorin-datasus/tbEstabelecimento201708.csv,s3a://fernando-bontorin-datasus/tbEstabelecimento201709.csv,s3a://fernando-bontorin-datasus/tbEstabelecimento201710.csv,s3a://fernando-bontorin-datasus/tbEstabelecimento201711.csv,s3a://fernando-bontorin-datasus/tbEstabelecimento201712.csv,s3a://fernando-bontorin-datasus/tbEstabelecimento201802.csv,s3a://fernando-bontorin-datasus/tbEstabelecimento201803.csv,s3a://fernando-bontorin-datasus/tbEstabelecimento201804.csv,s3a://fernando-bontorin-datasus/tbEstabelecimento201805.csv,s3a://fernando-bontorin-datasus/tbEstabelecimento201806.csv,s3a://fernando-bontorin-datasus/tbEstabelecimento201807.csv,s3a://fernando-bontorin-datasus/tbEstabelecimento201808.csv,s3a://fernando-bontorin-datasus/tbEstabelecimento201809.csv,s3a://fernando-bontorin-datasus/tbEstabelecimento201810.csv,s3a://fernando-bontorin-datasus/tbEstabelecimento201811.csv,s3a://fernando-bontorin-datasus/tbEstabelecimento201812.csv,s3a://fernando-bontorin-datasus/tbEstabelecimento201901.csv,s3a://fernando-bontorin-datasus/tbEstabelecimento201902.csv,s3a://fernando-bontorin-datasus/tbEstabelecimento201903.csv,s3a://fernando-bontorin-datasus/tbEstabelecimento201904.csv,s3a://fernando-bontorin-datasus/tbEstabelecimento201905.csv,s3a://fernando-bontorin-datasus/tbEstabelecimento201906.csv,s3a://fernando-bontorin-datasus/tbEstabelecimento201907.csv,s3a://fernando-bontorin-datasus/tbEstabelecimento201908.csv,s3a://fernando-bontorin-datasus/tbEstabelecimento201909.csv,s3a://fernando-bontorin-datasus/tbEstabelecimento201910.csv,s3a://fernando-bontorin-datasus/tbEstabelecimento201911.csv,s3a://fernando-bontorin-datasus/tbEstabelecimento201912.csv,s3a://fernando-bontorin-datasus/tbEstabelecimento202001.csv,s3a://fernando-bontorin-datasus/tbEstabelecimento202002.csv,s3a://fernando-bontorin-datasus/tbEstabelecimento202003.csv,s3a://fernando-bontorin-datasus/tbEstabelecimento202004.csv,s3a://fernando-bontorin-datasus/tbEstabelecimento202005.csv,s3a://fernando-bontorin-datasus/tbEstabelecimento202006.csv,s3a://fernando-bontorin-datasus/tbEstabelecimento202007.csv,s3a://fernando-bontorin-datasus/tbEstabelecimento202008.csv,s3a://fernando-bontorin-datasus/tbEstabelecimento202009.csv,s3a://fernando-bontorin-datasus/tbEstabelecimento202010.csv,s3a://fernando-bontorin-datasus/tbEstabelecimento202011.csv,s3a://fernando-bontorin-datasus/tbEstabelecimento202012.csv,s3a://fernando-bontorin-datasus/tbEstabelecimento202101.csv,s3a://fernando-bontorin-datasus/tbEstabelecimento202102.csv,s3a://fernando-bontorin-datasus/tbEstabelecimento202103.csv,s3a://fernando-bontorin-datasus/tbEstabelecimento202104.csv,s3a://fernando-bontorin-datasus/tbEstabelecimento202105.csv,s3a://fernando-bontorin-datasus/tbEstabelecimento202106.csv,s3a://fernando-bontorin-datasus/tbEstabelecimento202107.csv,s3a://fernando-bontorin-datasus/tbEstabelecimento202108.csv,s3a://fernando-bontorin-datasus/tbEstabelecimento202109.csv,s3a://fernando-bontorin-datasus/tbEstabelecimento202110.csv,s3a://fernando-bontorin-datasus/tbEstabelecimento202111.csv,s3a://fernando-bontorin-datasus/tbEstabelecimento202112.csv --output s3a://fernando-bontorin-datasus/tbEstabelecimentoFull --google-apis-token