import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import static java.lang.System.out;

/**
 * ConnHiveByJDBCDemo
 *
 * @author SF2121
 */
public class ConnHiveByJDBCDemo {

    private static String HIVE_SERVER2_ADDRESS = "10.0.8.8";
    private static String HIVE_DATABASE_NAME = "beeline_spark2";

    private static Method addURL = initAddMethod();

    private static URLClassLoader classloader = (URLClassLoader) ClassLoader.getSystemClassLoader();

    public static void main(String[] arg) {
        File file = new File("");

        String path = file.getAbsolutePath() + "/applet-hive";

        hive(path);
    }

    public static void hive(String path) {
        Connection con = null;
        Statement stmt = null;
        ResultSet res = null;
        try {
            File f = new File(path + "/lib");
            File[] files = f.listFiles();
            for (File jar : files) {
                addURL.invoke(classloader, new Object[]{jar.toURI().toURL()});
            }

            Class.forName("org.apache.hive.jdbc.HiveDriver");

            //con = DriverManager.getConnection("jdbc:hive2://" + HIVE_SERVER2_ADDRESS + ":10016/" + HIVE_DATABASE_NAME, "hive", "");
            con = DriverManager.getConnection("jdbc:hive2://" + HIVE_SERVER2_ADDRESS + ":10000", "hive", "");
            stmt = con.createStatement();
            File file = new File(path + "/input.sql");
            StringBuilder sb = new StringBuilder();
            BufferedReader reader = new BufferedReader(new FileReader(file));
            String line;
            while ((line = reader.readLine()) != null) {
                sb.append(line).append(System.getProperty("line.separator"));
            }
            reader.close();
            String sql = sb.toString().trim();
            out.println(" -[DEBUG]-   " + sql);
            File outFile = new File(path + "/output.csv");
            BufferedWriter writer = new BufferedWriter(new FileWriter(outFile));
            if (sql.toUpperCase().startsWith("SELECT") || sql.toUpperCase().startsWith("SHOW")) {
                res = stmt.executeQuery(sql);
                ResultSetMetaData meta = res.getMetaData();
                int cols = meta.getColumnCount();
                StringBuilder row1 = new StringBuilder();
                for (int i = 1; i <= cols; i++) {
                    String columnName = meta.getColumnLabel(i);
                    if (null == columnName || 0 == columnName.length()) {
                        columnName = meta.getColumnName(i);
                    }
                    row1.append(columnName).append(",");
                }
                row1.append(System.getProperty("line.separator"));
                writer.write(row1.toString());
                while (res.next()) {
                    StringBuilder row = new StringBuilder();
                    for (int i = 1; i <= cols; i++) {
                        String typeName = meta.getColumnTypeName(i).toUpperCase();
                        if (typeName.contains("TIME") || typeName.contains("DATE")) {
                            row.append(convertBlankStr(res.getString(i))).append(",");
                        } else {
                            row.append(convertBlankStr(String.valueOf(res.getObject(i)))).append(",");
                        }
                    }
                    row.append(System.getProperty("line.separator"));
                    writer.write(row.toString());
                }
            } else {
                int i = stmt.executeUpdate(sql);
                writer.write("influence: " + i + " line " + System.getProperty("line.separator"));
            }

            writer.close();
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        } finally {

            if (res != null) {
                try {
                    res.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (con != null) {
                try {
                    con.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private static Method initAddMethod() {
        try {
            Method add = URLClassLoader.class.getDeclaredMethod("addURL", new Class[]{URL.class});
            add.setAccessible(true);
            return add;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static String convertBlankStr(String str) {
        if (null == str || "null".equals(str)) {
            str = "";
        }
        return str;
    }
}
