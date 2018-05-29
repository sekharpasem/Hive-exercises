import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.DriverManager;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.bson.Document;

import com.config.Config;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class Connections {
	private static String driverName = "org.apache.hive.jdbc.HiveDriver";
	private static Connection hiveConnection = null;
	private static MongoDatabase database;
	static {
		try {
			getMongoMongoDB();
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
	}

	public static MongoCollection<Document> getMongoCollection(String collectionName) {
		return database.getCollection(collectionName);
	}

	private static MongoDatabase getMongoMongoDB() throws UnknownHostException {
		System.setProperty("javax.net.ssl.trustStore", Config.getConfig().getTruststorePath());
		System.setProperty("javax.net.ssl.trustStorePassword", Config.getConfig().getTruststorePass());
		MongoClientURI connectionString = new MongoClientURI(Config.getConfig().getMongo_uri());
		MongoClient mongoClient = new MongoClient(connectionString);
		database = mongoClient.getDatabase(connectionString.getDatabase());
		return database;

	}

	public static Connection getHiveConnection() throws Exception {
		try {
			if (hiveConnection == null) {
				org.apache.hadoop.conf.Configuration conf = new Configuration();
				conf.set("hadoop.security.authentication", Config.getConfig().getHadoop_security_authentication());
				conf.addResource(new Path(Config.getConfig().getHive_site_xml_path()));

				System.setProperty("java.security.krb5.conf", Config.getConfig().getJava_security_krb5_conf());
				System.setProperty("sun.security.krb5.debug", Config.getConfig().getSun_security_krb5_debug());

				String principal = System.getProperty("kerberosPrincipal", Config.getConfig().getKerberosPrincipal());
				String keytabLocation = System.getProperty("kerberosKeytab", Config.getConfig().getKerberosKeytab());

				UserGroupInformation.setConfiguration(conf);
				UserGroupInformation.loginUserFromKeytab(principal, keytabLocation);
				Class.forName(driverName);
				// replace "hive" here with the name of the user the queries should run as
				//String url = "jdbc:hive2://ahlclotxpla701.evv1.ah-isd.net:10000/default;principal=hive/ahlclotxpla701.evv1.ah-isd.net@DS.SJHS.COM;ssl=true;sslTrustStore=/opt/cloudera/security/jks/keystore.jks";
				hiveConnection = DriverManager.getConnection(Config.getConfig().getHive_jdbc_uri());
			}
		} catch (Exception e) {
			e.printStackTrace();
			throw e;
		}
		return hiveConnection;
	}

	public static void main(String args[]) {
		MongoCollection<Document> collection=getMongoCollection("metrics");
		System.out.println(collection.find());
	}
}
