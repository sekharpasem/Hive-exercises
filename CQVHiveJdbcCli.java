import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;

import com.config.Config;

public class CQVHiveJdbcCli {
	public static void main(String[] args) throws Exception {
		Connection con = Connections.getHiveConnection();
		Config config=Config.getConfig();
		System.out.println(con);

		ResultSet countSet = con.createStatement()
				.executeQuery("SELECT COUNT(*) FROM dtz_match_exception.dtz_match_exception");
		if (countSet.next()) {
			System.out.println("Received number of rows: " + countSet.getString(1));
		}

		String query = "SELECT * FROM dtz_match_exception.dtz_match_exception limit 3000";
		ResultSet resultSet = con.createStatement().executeQuery(query);

		ResultSetMetaData metaData = resultSet.getMetaData();
		int columns = metaData.getColumnCount();
		System.out.println(columns);
		StringBuilder builder = new StringBuilder();
		if (resultSet != null) {
			for(int a = 1; a <= columns; a++) {
				builder.append(metaData.getColumnName(a));
				if(a != columns) {
					builder.append("|");
				}
			}
			builder.append("\n");
			while (resultSet.next()) {
				for (int i = 1; i <= columns; i++) {
					String modSet = resultSet.getString(i);
					if(modSet == null) {
						builder.append("null");
					}
					else if(modSet.toLowerCase().equals("true")) {
						builder.append("1");
					}
					else if(modSet.toLowerCase().equals("false")) {
						builder.append("0");
					}
					else {
						builder.append(modSet);
					}
					if(i != columns) {
						builder.append("|");
					}
				}
				builder.append("\n");
			}
			writeToFile(config.getInputDatasetFileName(), builder);
		}

	}

	public static void writeToFile(String path, StringBuilder builder) throws IOException {
		BufferedWriter bwr = new BufferedWriter(new FileWriter(new File(path)));
		bwr.write(builder.toString());
		bwr.flush();
		bwr.close();
	}

}
