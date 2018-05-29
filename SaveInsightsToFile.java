import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.reflect.Type;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.apache.commons.collections4.CollectionUtils;
import org.bson.Document;

import com.config.Config;
import com.constants.ConfigConstants;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;
import com.model.Prediction;

public class SaveInsightsToFile {

	static Config config = Config.getConfig();

	public static void main(String args[]) {
		try {

			SimpleDateFormat sdf = new SimpleDateFormat(ConfigConstants.DATE_FORMAT_DD_MM_YYYY_H_M_S);
			Date startDate = new Date();
			System.out.println("Start SaveInsightsToHive(main): at " + startDate);
			String startDtStr = sdf.format(startDate);
			// process predictions
			clearContentsofFile(Config.getConfig().getInsigtsOutputFilePath());
			writeToInsightsFileHeader();
			Document metricsDoc = processPredictions();

			Date endDate = new Date();
			String endDtStr = sdf.format(endDate);

			if (config.getSave_metrics_to_mongo()) {
				metricsDoc.append(ConfigConstants.METRIC_DATE_TIMESTAMP,
						new SimpleDateFormat(ConfigConstants.DATE_FORMAT_DD_MM_YYYY).format(new Date()));
				metricsDoc.append(ConfigConstants.METRIC_START_TIMESTAMP, startDtStr);
				metricsDoc.append(ConfigConstants.METRIC_END_TIMESTAMP, endDtStr);
				totalInvoicesProcessedPercentage(metricsDoc);
				System.out.println("SaveInsightsToHive(main) : Saving metrics to mongo...");
				saveDocumentToMongo(metricsDoc, config.getMetrics_collection_name());
			}

			System.out.println("End SaveInsightsToHive(main): at " + endDtStr);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * reads predictions from file and stores to hive
	 * 
	 * @return
	 * 
	 * @throws Exception
	 */
	public static Document processPredictions() throws Exception {
		Type REVIEW_TYPE = new TypeToken<List<MeException>>() {
		}.getType();
		// long beforeRowsCount = insightsTableRecordsCount();
		// System.out.println(" Before inserting...count : " + beforeRowsCount);
		Gson gson = new Gson();
		JsonReader reader = null;
		try {
			reader = new JsonReader(new FileReader(Config.getConfig().getMlModelOutputPath()));
		} catch (Exception e) {
			System.out.println("No insights were generated!");
			// constructTruncateQuery();
			Document metricsDoc = new Document(ConfigConstants.METRICS_TOTAL_INSIGHTS_GENERATED, 0);
			metricsDoc.append(ConfigConstants.METRICS_TOTAL_RECORDS, 0);
			metricsDoc.append(ConfigConstants.METRIC_TOTAL_NON_PREDICTED_RECORDS, 0);
			metricsDoc.append(ConfigConstants.METRIC_TOTAL_LOW_INSIGHTS, 0);
			metricsDoc.append(ConfigConstants.METRIC_TOTAL_HIGH_INSIGHTS, 0);
			metricsDoc.append(ConfigConstants.METRIC_TOTAL_MEDIUM_INSIGHTS, 0);
			return metricsDoc;
		}
		List<MeException> jsonDataList = gson.fromJson(reader, REVIEW_TYPE);
		List<String> predictedInvoicesIdList = new LinkedList<String>();
		List<String> rows = new LinkedList<String>();
		String invoiceId;
		String row = null;
		// constructTruncateQuery();
		long total_insights_count = 0;
		long lowCount = 0;
		long highCount = 0;
		long mediumCount = 0;

		for (MeException meException : jsonDataList) {
			invoiceId = meException.getID();
			System.out.println("Processing prediction for invoiceId: " + invoiceId);
			predictedInvoicesIdList.add(invoiceId);
			List<Prediction> predictions = meException.getPredictions();
			for (Prediction prediction : predictions) {

				if (prediction.getEvidence() == null || prediction.getConfidence().equals("Null"))
					continue;
				else {
					if (prediction.getConfidence().equalsIgnoreCase("Low"))
						lowCount++;
					if (prediction.getConfidence().equalsIgnoreCase("High"))
						highCount++;
					if (prediction.getConfidence().equalsIgnoreCase("Medium"))
						mediumCount++;

					row = buildRow(invoiceId, prediction);
					rows.add(row);
					total_insights_count += 1;
					saveIfBatchReachesMax(rows, false);
				}

			}
		}

		Set<String> totalInvoiceIds = getTotalInvoidsList(Config.getConfig().getInputDatasetFileName());
		Collection<String> nonProcessedIds = CollectionUtils.subtract(totalInvoiceIds, predictedInvoicesIdList);
		long totalInvoiceIdsCount = totalInvoiceIds.size();
		long predictedInvoicesIdsCount = predictedInvoicesIdList.size();
		long nonProcessedIdsCount = 0;
		if (nonProcessedIds != null) {
			total_insights_count += nonProcessedIds.size();
			nonProcessedIdsCount = nonProcessedIds.size();
			for (String nonProcessedId : nonProcessedIds) {
				row = buildRow(nonProcessedId, null);
				rows.add(row);
				saveIfBatchReachesMax(rows, false);
			}
		}
		// saves remaining records
		saveIfBatchReachesMax(rows, true);
		System.out.format("Total number of insights with confidence low = %d, medium = %d, and high = %d.\n", lowCount,
				mediumCount, highCount);

		System.out.println("Total invoice ids from input dataset file are:" + totalInvoiceIdsCount);
		System.out.println("Total match exception ids processed from ml model are:" + predictedInvoicesIdsCount);
		System.out.println("Total Non predicted invoices ids are: " + nonProcessedIdsCount);
		System.out.println("Total insights saved are: " + total_insights_count);
		// long afterRowsCount = insightsTableRecordsCount();
		// System.out.println(" After inserting...count : " + afterRowsCount);
		Document metricsDoc = new Document(ConfigConstants.METRICS_TOTAL_INSIGHTS_GENERATED, total_insights_count);
		metricsDoc.append(ConfigConstants.METRICS_TOTAL_RECORDS, totalInvoiceIdsCount);
		metricsDoc.append(ConfigConstants.METRIC_TOTAL_NON_PREDICTED_RECORDS,
				nonProcessedIds == null ? Long.valueOf(0) : Long.valueOf(nonProcessedIds.size()));
		metricsDoc.append(ConfigConstants.METRIC_TOTAL_LOW_INSIGHTS, lowCount);
		metricsDoc.append(ConfigConstants.METRIC_TOTAL_HIGH_INSIGHTS, highCount);
		metricsDoc.append(ConfigConstants.METRIC_TOTAL_MEDIUM_INSIGHTS, mediumCount);
		return metricsDoc;
	}

	/**
	 * builds row values of prediction
	 * 
	 * @param invoiceId
	 * @param prediction
	 * @return
	 */
	public static String buildRow(String invoiceId, Prediction prediction) {

		StringBuilder builder = new StringBuilder();
		// builder.append("(");
		wrapDelimeter(UUID.randomUUID().toString(), builder, true);

		wrapDelimeter(invoiceId, builder, true);
		if (prediction == null) {
			wrapDelimeter(null, builder, true);
		} else {
			wrapDelimeter(prediction.getMe_code(), builder, true);
		}
		if (prediction == null) {
			wrapDelimeter(null, builder, true);
		} else {
			wrapDelimeter(prediction.getConfidence(), builder, true);
		}
		// allow_mongo_insertion config
		if (prediction == null) {
			wrapDelimeter(null, builder, true);
		} else {
			if (Config.getConfig().getAllow_mongo_insertion())
				wrapDelimeter(String.join(",", prediction.getMissing_ME()), builder, true);
			else
				wrapDelimeter(null, builder, true);
		}
		// prescription
		wrapDelimeter(null, builder, true);
		if (prediction == null) {
			wrapDelimeter(null, builder, true);
		} else {
			wrapDelimeter(String.valueOf(prediction.getScore()), builder, true);
		}
		if (prediction == null) {
			wrapDelimeter(null, builder, true);
		} else {
			wrapDelimeter(String.valueOf(prediction.getThreshold1()), builder, true);
		}
		if (prediction == null) {
			wrapDelimeter(null, builder, true);
		} else {
			wrapDelimeter(String.valueOf(prediction.getThreshold2()), builder, true);
		}
		if (prediction == null) {
			wrapDelimeter(null, builder, true);
		} else {
			wrapDelimeter(String.valueOf(prediction.getThreshold3()), builder, true);
		}
		// allow_mongo_insertion config
		if (prediction == null) {
			wrapDelimeter(null, builder, true);
		} else {
			if (Config.getConfig().getAllow_mongo_insertion()) {
				wrapDelimeter(String.valueOf(prediction.getBinary_threshold()), builder, true);
			} else
				wrapDelimeter(null, builder, true);
		}
		if (prediction == null || prediction.getEvidence() == null || prediction.getEvidence().isEmpty()) {
			for (int i = 0; i < 10; i++)
				wrapDelimeter(null, builder, true);
		} else {
			List<String> evidences = prediction.getEvidence();
			if (evidences != null) {
				for (int i = 0; i < 10; i++) {
					String contributingFactor = null;
					if (evidences.size() > i) {
						contributingFactor = evidences.get(i);
					}
					wrapDelimeter(contributingFactor, builder, true);
				}
			}

		}
		Date date = new Date();
		SimpleDateFormat sdf = new SimpleDateFormat(ConfigConstants.DATE_FORMAT_DD_MM_YYYY);
		String strDate = sdf.format(date);
		wrapDelimeter(strDate, builder, false);
		return builder.toString();
	}

	/**
	 * save all inserts
	 * 
	 * @param rowsList
	 * @param checkForBatchSize
	 */
	public static void saveIfBatchReachesMax(List<String> rowsList, boolean checkForBatchSize) {
		if ((rowsList.size() >= Config.getConfig().getMax_batch_size()) || checkForBatchSize) {
			try {
				// call insert
				StringBuilder builder = new StringBuilder();
				for (String row : rowsList) {
					builder.append(row);
					builder.append("\n");
				}
				writeToFile(Config.getConfig().getInsigtsOutputFilePath(), builder);
				rowsList.clear();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}


	/***
	 * gets invoiceids from file
	 * 
	 * @param inputDatasetPath
	 * @return
	 */
	public static Set<String> getTotalInvoidsList(String inputDatasetPath) {

		BufferedReader br = null;
		String line = "";
		String delimeter = "\\|";
		Set<String> totalInvoiceIds = new LinkedHashSet<String>();
		boolean hasHeaderProcessed = false;
		try {

			br = new BufferedReader(new FileReader(inputDatasetPath));
			while ((line = br.readLine()) != null) {
				if (!hasHeaderProcessed) {
					hasHeaderProcessed = true;
					continue;
				}
				String[] row = line.split(delimeter);
				if (row.length > 0 && (row[0] != null && !row[0].equals("NULL")))
					totalInvoiceIds.add(row[0]);
			}

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (br != null) {
				try {
					br.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		return totalInvoiceIds;
	}

	/**
	 * wraps delimeter around string
	 * 
	 * @param str
	 * @param builder
	 * @param addCommaEnd
	 * @param isFloat
	 */
	public static void wrapDelimeter(String str, StringBuilder builder, boolean addCommaEnd) {
		if (str != null) {
			builder.append(str);
		} else {
			builder.append("null");
		}
		if (addCommaEnd) {
			builder.append("|");
		}
	}

	/**
	 * saves document to mongo
	 * 
	 * @param document
	 * @param collection
	 */
	public static void saveDocumentToMongo(Document document, String collection) {
		try {
			if (document != null && collection != null) {
				Connections.getMongoCollection(collection).insertOne(document);
			}
		} catch (Exception e) {
			System.out.println("Error: While saving document to mongo...\n" + e.getMessage());
		}
	}

	public static void totalInvoicesProcessedPercentage(Document metric) {
		System.out.println("total invoices processed percentage method...");
		if (metric != null) {
			long totalInvoicesMissed = 0;
			long totalInvoices = 0;
			long total_invoiceids = metric.getLong(ConfigConstants.METRICS_TOTAL_RECORDS);
			long total_non_predicted = metric.getLong(ConfigConstants.METRIC_TOTAL_NON_PREDICTED_RECORDS);
			totalInvoicesMissed += (total_invoiceids - total_non_predicted);
			totalInvoices += total_invoiceids;
			try {
				metric.append(ConfigConstants.METRIC_TOTAL_INVOICES_PERC, (totalInvoicesMissed / totalInvoices) * 100);
			} catch (ArithmeticException ae) {
				metric.append(ConfigConstants.METRIC_TOTAL_INVOICES_PERC, 0);
				System.out.println("ArithmeticException occured! because totalInvoiceIds zero...");
			}
		}
	}

	private static void writeToFile(String path, StringBuilder builder) throws IOException {
		BufferedWriter bwr = new BufferedWriter(new FileWriter(new File(path), true));
		try {
			bwr.write(builder.toString());
			bwr.flush();
		} catch (Exception e) {
			System.out.println("Error:" + e.getMessage());
		} finally {
			bwr.close();
		}
	}

	private static void writeToInsightsFileHeader() throws IOException {
		StringBuilder builder = new StringBuilder();
		builder.append(ConfigConstants.INSIGHTS_HEADERS);
		builder.append("\n");
		writeToFile(Config.getConfig().getInsigtsOutputFilePath(), builder);
	}

	private static void clearContentsofFile(String path) throws IOException {
		try {
			System.out.println("Clearing contents of file if already exists");
			PrintWriter pw = new PrintWriter(path);
			pw.close();
		} catch (Exception e) {
			System.out.println("Error! " + e.getMessage());
		}
	}
}
