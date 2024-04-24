import com.mysql.cj.jdbc.MysqlDataSource;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.Scanner;

public class Main {

    public static void main(String[] args) {
        var props = new Properties();

        try {
            props.load(Files.newInputStream(Path.of("music.properties"),
                    StandardOpenOption.READ));
            props.load(new FileInputStream("music.properties"));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        var dataSource = new MysqlDataSource();
        dataSource.setServerName(props.getProperty("serverName"));
        dataSource.setPort(Integer.parseInt(props.getProperty("port")));
        dataSource.setDatabaseName(props.getProperty("databaseName"));
        try (
                var connection = dataSource.getConnection(
                        props.getProperty("user"),
                        /*System.getenv("MYSQL_PASS")*/ "1234567890Qw"
                );
        Statement statement = connection.createStatement();
        ) {
            System.out.println("Conexion realizada con exito");
            String tableName = "music.artists",
                    columnName = "artist_name",
                    columnValue = "Bob Dylan";

            String[] columnNames = {"track_number", "song_title", "album_id"},
                    columnValues = {"1", "song2", "1"},
                    songs = {"R1", "R2", "R3"};

            insertRecord(statement, "music.songs", columnNames, columnValues);
            System.out.println("1 insertRecord OK");
            insertArtistAlbum(statement, "Roberto Carlos", "Camino", songs);
            System.out.println("1 insertArtistAlbum OK");
            deleteRecord(statement, tableName, columnName, "Mehitabel"); //TODO
            System.out.println("1 deleteRecord OK");
            insertRecord(statement, tableName, columnNames, columnValues);          //TODO
            System.out.println("2 insertRecord OK");
            updateRecord(statement, tableName, columnName, "Rodrigo R", columnName, "Rodrigo Roman");
            System.out.println("1 updateRecord OK");
            deleteRecord(statement, tableName, "artist_name", "AAAA");
            System.out.println("2 deleteRecord OK");

            if (!executeSelect(statement, tableName, columnName, columnValue)) {
                executeSelect(statement, "music.artists", "artist_id", "2 or artist_id=4 ");
            }

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }


    /**
     * Imprime los datos de una tabla especificada
     *
     * @param resultSet la tabla a imprimir
     * @throws SQLException en caso de cualquier malformación en resultSet
     */
    private static boolean printRecords(ResultSet resultSet) throws SQLException {
        boolean foundData = false;
        var meta = resultSet.getMetaData();
        System.out.println("=".repeat(20));
        for (int i = 1; i < meta.getColumnCount(); i++) {
            System.out.printf("%-15s", meta.getColumnName(i).toUpperCase());
        }
        System.out.println();

        while (resultSet.next()) {
            for (int i = 1; i <= meta.getColumnCount(); i++) {
                System.out.printf("%-15s ", resultSet.getString(i));
            }
            System.out.println();
            foundData = true;
        }
        return foundData;
    }


    /**
     * Hace un select a la base de datos con la clausula "WHERE columnName=columnValue"
     *
     * @param statement   la sesión a la base de datos
     * @param table       la tabla a hacer el select
     * @param columnName  el nombre de la columna a hacer el where
     * @param columnValue el valor de la columna donde se aplica el where
     * @throws SQLException en caso de cualquier malformación en el query
     */
    private static boolean executeSelect(Statement statement, String table,
                                         String columnName, String columnValue) throws SQLException {
        String query = "SELECT * FROM " + table + " WHERE " + columnName + "='" + columnValue + "'";
        var resultSet = statement.executeQuery(query);
        if (resultSet != null) {
            return printRecords(resultSet);
        }
        return false;
    }


    /**
     * Inserta un nuevo registro a una tabla especificada
     *
     * @param statement    la sesión de la base de datos
     * @param table        tabla a escribir el registro
     * @param columnNames  el nombre de las columnas a insertar
     * @param columnValues el valor de los columnas a insertar
     * @throws SQLException en caso de cualquier malformación en el query
     */
    private static boolean insertRecord(Statement statement, String table,
                                        String[] columnNames, String[] columnValues) throws SQLException {

        String[] fixedColumnValues = columnValues.clone();

        for (int i = 0; i < fixedColumnValues.length; i++) {
            try {
                Integer.parseInt(fixedColumnValues[i]);
            } catch (Exception e) {
                fixedColumnValues[i] = "'" + fixedColumnValues[i] + "'";
            }
        }

        String colNames = String.join(",", columnNames),
                colValues = String.join(",", fixedColumnValues);

        String query = "INSERT INTO " + table + " (" + colNames + ") VALUES (" + colValues + ")";
        System.out.println(query);
        boolean insertResult = statement.execute(query);
        int recordsInserted = statement.getUpdateCount();
        if (recordsInserted > 0) {
            executeSelect(statement, table, columnNames[0], fixedColumnValues[0]);
        }

        return recordsInserted > 0;
    }


    /**
     * Borra un registro de la base de datos
     *
     * @param statement   la sesión a la base de datos
     * @param table       la tabla donde hacer el DELETE
     * @param columnName  el nombre de la columna a hacer DELETE
     * @param columnValue el valor de la columna
     */
    private static boolean deleteRecord(Statement statement, String table,
                                        String columnName, String columnValue) throws SQLException {
        String query = "DELETE FROM " + table + " WHERE " + columnName + "='" + columnValue + "'";
        System.out.println(query);
        statement.execute(query);
        int recordsDeleted = statement.getUpdateCount();
        if (recordsDeleted > 0) {
            executeSelect(statement, table, columnName, columnValue);
        }
        return recordsDeleted > 0;
    }


    /**
     * Actualiza los valores de un registro en una tabla
     *
     * @param statement     la sesión de la base de datos
     * @param table         tabla a modificar
     * @param matchedColumn la columna a modificar
     * @param matchedValue  el valor del registro a modificar
     * @param updateColumn  la nueva columna
     * @param updateValue   el nuevo valor
     * @throws SQLException en caso de cualquier malformación en el query
     */
    private static boolean updateRecord(Statement statement, String table,
                                        String matchedColumn, String matchedValue,
                                        String updateColumn, String updateValue) throws SQLException {
        String query = "UPDATE " + table + " SET " + updateColumn + " ='" + updateValue + "' WHERE " + matchedColumn + "='" + matchedValue + "'";
        System.out.println(query);
        statement.execute(query);
        int recordsUpdated = statement.getUpdateCount();
        if (recordsUpdated > 0) {
            executeSelect(statement, table, updateColumn, updateValue);
        }
        return recordsUpdated > 0;
    }


    /**
     * Inserta un nuevo alnum de algún artista
     *
     * @param statement  la sesión a la base de datos
     * @param artistName el nombre del artista
     * @param albumName  el nombre del album
     * @param songs      los nombres de las canciones
     * @throws SQLException en caso de alguna malformación en el query
     */
    private static void insertArtistAlbum(Statement statement,
                                          String artistName,
                                          String albumName, String[] songs)
            throws SQLException {

        String artistInsert = "INSERT INTO music.artists (artist_name) VALUES (" + statement.enquoteLiteral(artistName) + ")";
        System.out.println(artistInsert);
        statement.execute(artistInsert, Statement.RETURN_GENERATED_KEYS);

        ResultSet rs = statement.getGeneratedKeys();
        int artistId = (rs != null && rs.next()) ? rs.getInt(1) : -1;
        String albumInsert = "INSERT INTO music.albums (album_name, artist_id)" +
                " VALUES (" + statement.enquoteLiteral(albumName) + ", " + artistId + ")";
        System.out.println(albumInsert);
        statement.execute(albumInsert, Statement.RETURN_GENERATED_KEYS);
        rs = statement.getGeneratedKeys();
        int albumId = (rs != null && rs.next()) ? rs.getInt(1) : -1;

        String songInsert = "INSERT INTO music.songs " +
                "(track_number, song_title, album_id) VALUES (%d, %s, %d)";

        for (int i = 0; i < songs.length; i++) {
            String songQuery = songInsert.formatted(i + 1,
                    statement.enquoteLiteral(songs[i]), albumId);
            System.out.println(songQuery);

            statement.execute(songQuery);
        }
    }
}