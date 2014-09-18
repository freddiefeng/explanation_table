package hive;
import java.sql.SQLException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.DriverManager;

public class FlashlightETB {
    private static String driverName = "org.apache.hive.jdbc.HiveDriver";
    private static String tableName = "income";
    private long numSummary = 0;

    public static void main(String[] args) throws SQLException {
        try {
            Class.forName(driverName);
        } catch (ClassNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            System.exit(1);
        }

        Connection con = DriverManager.getConnection("jdbc:hive2://localhost:10000/default", "freddie", "");
        Statement stmt = con.createStatement();
        ResultSet res;
//        stmt.executeUpdate("use default");
        res = stmt.executeQuery("show tables");
        while (res.next()) {
            System.out.println(res.getString(1) + "\n");
        }
        stmt.executeUpdate("drop table " + tableName + "_summary");
        stmt.executeUpdate(
                "create table " + tableName + "_summary(" +
                        "a1 varchar(64)," +
                        "a2 varchar(64)," +
                        "a3 varchar(64)," +
                        "a4 varchar(64)," +
                        "a5 varchar(64)," +
                        "a6 varchar(64)," +
                        "a7 varchar(64)," +
                        "a8 varchar(64)," +
                        "a9 varchar(64)," +
                        "support float," +
                        "observation float," +
                        "multiplier float," +
                        "gain float," +
                        "id bigint," +
                        "kl float" +
                        ")"
        );
    }
}
