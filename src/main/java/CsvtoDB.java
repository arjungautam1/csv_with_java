
import java.io.*;
import java.sql.*;

public class CsvtoDB {

    public static void main(String[] args) {
        String jdbcURL = "jdbc:mysql://localhost:3306/bit";
        String username = "root";
        String password = "";

        String csvFilePath = "src/main/data.csv";

        int batchSize = 20;

        Connection connection = null;

        try {

            connection = DriverManager.getConnection(jdbcURL, username, password);
            connection.setAutoCommit(false);

            String sql = "INSERT INTO student VALUES (?, ?, ?, ?, ?,?, ?, ?, ?, ?,?, ?, ?, ?, ?,?,?)";
            PreparedStatement statement = connection.prepareStatement(sql);

            BufferedReader lineReader = new BufferedReader(new FileReader(csvFilePath));
            String lineText = null;

            int count = 0;

            lineReader.readLine(); // skip header line

            while ((lineText = lineReader.readLine()) != null) {
                String[] data = lineText.split(",");
                String course_id = data[0];
                String unit_id = data[1];
                String unit_desc = data[2];
                String assessment_id = data[3];
                String student_id = data[4];
                String campus_id = data[5];
                String criteria = data[6];
                String ga_id = data[7];
                String score = data[8];
                String category = data[9];
                String weight = data[10];
                String learing_stage = data[11];
                String aspect = data[12];
                String iteration = data[13];
                String year = data[14];
                String semester = data[15];
                String aol_flag = data[16];

                statement.setString(1, course_id);
                statement.setString(2, unit_id);
                statement.setString(3, unit_desc);
                statement.setInt(4, Integer.parseInt(assessment_id));
                statement.setString(5, student_id);
                statement.setString(6, campus_id);
                statement.setString(7, criteria);
                statement.setString(8, ga_id);
                statement.setString(9, score);
                statement.setString(10, category);
                statement.setString(11, weight);
                statement.setString(12, learing_stage);
                statement.setInt(13, Integer.parseInt(aspect));
                statement.setInt(14, Integer.parseInt(iteration));
                statement.setInt(15, Integer.parseInt(year));
                statement.setInt(16,Integer.parseInt(semester));
                statement.setInt(17, Integer.parseInt(aol_flag));


                statement.addBatch();

                if (count % batchSize == 0) {
                    statement.executeBatch();
                }
            }

            lineReader.close();

            // execute the remaining queries
            statement.executeBatch();

            connection.commit();
            connection.close();

        } catch (IOException ex) {
            System.err.println(ex);
        } catch (SQLException ex) {
            ex.printStackTrace();

            try {
                connection.rollback();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

    }
}
