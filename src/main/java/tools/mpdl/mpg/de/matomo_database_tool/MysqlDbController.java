package tools.mpdl.mpg.de.matomo_database_tool;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MysqlDbController {

  //TODO
//  final static String PURE_ITEM_URL = "pure.mpg.de/pubman/item/item_";
  final static String PURE_ITEM_URL = "qa.pure.mpdl.mpg.de/pubman/item/item_";
  final static String COMPONENT_PATH = "/component/";
  final static int SQL_LIMIT = 1000;
  Connection connection = null;


  public MysqlDbController(String dbUrl) throws SQLException {
    super();
    System.out.println("Connecting...");
    this.connection = DriverManager.getConnection("jdbc:mysql://" + dbUrl + "?zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=UTC");
    System.out.println("Connection esatablished");
  }
  
  public MysqlDbController(String userName, String userPassword, String dbUrl) throws SQLException {
    super();
    System.out.println("Connecting...");
    this.connection = DriverManager.getConnection("jdbc:mysql://" + dbUrl + "?" + "user=" + userName
        + "&password=" + userPassword + "&zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=UTC");
    System.out.println("Connection esatablished");
  }

  public void updateLogActionTableEsciDoc() throws Exception {

    int offset = 0;
    boolean hasMoreResults = true;
    int resultIdAction = 0;
    String resultName = null;
    Pattern pattern = Pattern.compile("escidoc:\\d+", Pattern.CASE_INSENSITIVE);
    Matcher matcher = null;
    PreparedStatement statementSelect = null;
    PreparedStatement statementUpdate = null;
    statementSelect = connection.prepareStatement(
        "SELECT idaction, name, type, url_prefix FROM piwik_log_action WHERE (name REGEXP '^(http://|https://)?(pubman\\\\.mpdl\\\\.mpg\\\\.de|pure\\\\.mpg\\\\.de).*(/|=)+escidoc:(\\\\d)+:?(\\\\d)*')");
//        "SELECT idaction, name, type, url_prefix FROM piwik_log_action WHERE (name REGEXP '^(http://|https://)?(pubman\\\\.mpdl\\\\.mpg\\\\.de|pure\\\\.mpg\\\\.de).*(/|=)+escidoc:(\\\\d)+:?(\\\\d)*$') LIMIT ? OFFSET ?");
//    while (hasMoreResults) {
//      statementSelect.setInt(1, SQL_LIMIT);
//      statementSelect.setInt(2, offset);
      System.out.println("Sending statement...");

      ResultSet result = statementSelect.executeQuery();
      System.out.println("Processing Result...");
      int count = 0;
      int resultUpdate = 0;


      statementUpdate = connection.prepareStatement(
          "UPDATE piwik_log_action SET name = ?, url_prefix = ? WHERE idaction = ?;");
      while (result.next()) {
        resultIdAction = result.getInt("idaction");
        resultName = result.getString("name");
//        System.out.println(resultIdAction + " | " + resultName + " | " + result.getString("type"));
        matcher = pattern.matcher(resultName);
        String itemId = null;
        if (matcher.find()) {
          itemId = matcher.group();
//          System.out.println("Matched ID: " + itemId);
        }
        if (itemId != null) {
          String updatedName = PURE_ITEM_URL + itemId.substring(itemId.indexOf(":") + 1);
          statementUpdate.setString(1, updatedName);
          statementUpdate.setInt(2, 2); // set url_prefix to 2 (https://)
          statementUpdate.setInt(3, resultIdAction);
//          System.out.println(
//              "Updating [" + resultIdAction + "] from " + resultName + " to " + updatedName);
          resultUpdate = statementUpdate.executeUpdate();
          if (resultUpdate == 1) {
            System.out.println("Finished Updating [" + resultIdAction + "] from " + resultName
                + " to " + updatedName);
          } else {
            System.out.println("Error updating [" + resultIdAction + "] from " + resultName + " to "
                + updatedName);
          }

        }
        count++;
      }
      if (count < SQL_LIMIT) {
        System.out.println("finishing loops");
        hasMoreResults = false;
      }
//    offset += count;
//    System.out.println("Offset: " + offset);
    System.out.println("Count : " + count);
//    }

    System.out.println("Closing Connection...");

    statementSelect.close();
    statementSelect = null;
  }

  public void updateLogActionTableItem() throws Exception {

    int offset = 0;
    boolean hasMoreResults = true;
    int resultIdAction = 0;
    String resultName = null;
    Pattern pattern = Pattern.compile("item_\\d+", Pattern.CASE_INSENSITIVE);
    Matcher matcher = null;
    PreparedStatement statementSelect = null;
    PreparedStatement statementUpdate = null;
    statementSelect = connection.prepareStatement(
        "SELECT idaction, name, type, url_prefix FROM piwik_log_action WHERE (name REGEXP '^(http://|https://)?(pubman\\\\.mpdl\\\\.mpg\\\\.de|pure\\\\.mpg\\\\.de).*(/|=)+item_(\\\\d)+_?(\\\\d)*')");
//        "SELECT idaction, name, type, url_prefix FROM piwik_log_action WHERE (name REGEXP '^(http://|https://)?(pubman\\\\.mpdl\\\\.mpg\\\\.de|pure\\\\.mpg\\\\.de).*(/|=)+item_(\\\\d)+_?(\\\\d)*$') LIMIT ? OFFSET ?");
//    while (hasMoreResults) {
//      statementSelect.setInt(1, SQL_LIMIT);
//      statementSelect.setInt(2, offset);
      System.out.println("Sending statement...");

      ResultSet result = statementSelect.executeQuery();
      System.out.println("Processing Result...");
      int count = 0;
      int resultUpdate = 0;


      statementUpdate = connection.prepareStatement(
          "UPDATE piwik_log_action SET name = ?, url_prefix = ? WHERE idaction = ?;");
      while (result.next()) {
        resultIdAction = result.getInt("idaction");
        resultName = result.getString("name");
//        System.out.println(resultIdAction + " | " + resultName + " | " + result.getString("type"));
        matcher = pattern.matcher(resultName);
        String itemId = null;
        if (matcher.find()) {
          itemId = matcher.group();
//          System.out.println("Matched ID: " + itemId);
        }
        if (itemId != null) {
          String updatedName = PURE_ITEM_URL + itemId.substring(itemId.indexOf("_") + 1);
          statementUpdate.setString(1, updatedName);
          statementUpdate.setInt(2, 2); // set url_prefix to 2 (https://)
          statementUpdate.setInt(3, resultIdAction);
//          System.out.println(
//              "Updating [" + resultIdAction + "] from " + resultName + " to " + updatedName);
          resultUpdate = statementUpdate.executeUpdate();
          if (resultUpdate == 1) {
            System.out.println("Finished Updating [" + resultIdAction + "] from " + resultName
                + " to " + updatedName);
          } else {
            System.out.println("Error updating [" + resultIdAction + "] from " + resultName + " to "
                + updatedName);
          }

        }
        count++;
      }
      if (count < SQL_LIMIT) {
        System.out.println("finishing loop");
        hasMoreResults = false;
      }
//    offset += count;
//    System.out.println("Offset: " + offset);
    System.out.println("Count : " + count);
//    }

    System.out.println("Closing Connection...");

    statementSelect.close();
    statementSelect = null;
  }
  
  public void updateLogActionTableItemComponent() throws Exception {

    int offset = 0;
    boolean hasMoreResults = true;
    int resultIdAction = 0;
    String resultName = null;
    Pattern pattern = Pattern.compile("item_\\d+", Pattern.CASE_INSENSITIVE);
    // Before removing version number
//    Pattern pattern = Pattern.compile("item_\\d+_?\\d?", Pattern.CASE_INSENSITIVE);
    Matcher matcher = null;
    PreparedStatement statementSelect = null;
    PreparedStatement statementUpdate = null;
    statementSelect = connection.prepareStatement(
        "SELECT idaction, name, type, url_prefix FROM piwik_log_action WHERE (name REGEXP '^(http://|https://)?(pubman\\\\.mpdl\\\\.mpg\\\\.de|pure\\\\.mpg\\\\.de).*/item_(\\\\d)+_?(\\\\d)*/component+');");
//        "SELECT idaction, name, type, url_prefix FROM piwik_log_action WHERE (name REGEXP '^(http://|https://)?(pubman\\\\.mpdl\\\\.mpg\\\\.de|pure\\\\.mpg\\\\.de).*/item_(\\\\d)+_?(\\\\d)*/component+') LIMIT ? OFFSET ?;");
//    while (hasMoreResults) {
//      statementSelect.setInt(1, SQL_LIMIT);
//      statementSelect.setInt(2, offset);
      System.out.println("Sending statement...");

      ResultSet result = statementSelect.executeQuery();
      System.out.println("Processing Result...");
      int count = 0;
      int resultUpdate = 0;


      statementUpdate = connection.prepareStatement(
          "UPDATE piwik_log_action SET name = ?, url_prefix = ?, type = ? WHERE idaction = ?;");
      while (result.next()) {
        resultIdAction = result.getInt("idaction");
        resultName = result.getString("name");
//        System.out.println(resultIdAction + " | " + resultName + " | " + result.getString("type"));
        matcher = pattern.matcher(resultName);
        String itemId = null;
        if (matcher.find()) {
          itemId = matcher.group();
//          System.out.println("Matched ID: " + itemId);
        }
        if (itemId != null) {
          String restOfPath = resultName.substring(resultName.indexOf(COMPONENT_PATH) + 11); // substring after /component/
          String updatedName = null;
          if (restOfPath.contains("escidoc:") || restOfPath.contains("escidoc_")) {
            updatedName = PURE_ITEM_URL + (itemId.substring(itemId.indexOf("_") + 1)) + COMPONENT_PATH + "file_" + restOfPath.substring(restOfPath.indexOf("escidoc") + 8);
            // Before removing version number
            //            updatedName = PURE_ITEM_URL + (itemId.substring(itemId.indexOf("_") + 1)).replace(":", "_") + COMPONENT_PATH + "file_" + restOfPath.substring(restOfPath.indexOf("escidoc") + 8);
          } else {
            updatedName = PURE_ITEM_URL + (itemId.substring(itemId.indexOf("_") + 1)) + COMPONENT_PATH +  restOfPath;
         //     Before removing version number
            //            updatedName = PURE_ITEM_URL + (itemId.substring(itemId.indexOf("_") + 1)).replace(":", "_") + COMPONENT_PATH +  restOfPath;
          }
          statementUpdate.setString(1, updatedName);
          statementUpdate.setInt(2, 2); // set url_prefix to 2 (https://)
          statementUpdate.setInt(3, 3); // set type to 3 (download)
          statementUpdate.setInt(4, resultIdAction);
//          System.out.println(
//              "Updating [" + resultIdAction + "] from " + resultName + " to " + updatedName);
          resultUpdate = statementUpdate.executeUpdate();
          if (resultUpdate == 1) {
            System.out.println("Finished Updating [" + resultIdAction + "] from " + resultName
                + " to " + updatedName);
          } else {
            System.out.println("Error updating [" + resultIdAction + "] from " + resultName + " to "
                + updatedName);
          }

        }
        count++;
      }
      if (count < SQL_LIMIT) {
        System.out.println("finishing loop");
        hasMoreResults = false;
      }
//    offset += count;
//    System.out.println("Offset: " + offset);
    System.out.println("Count : " + count);
//    }

    System.out.println("Closing Connection...");

    statementSelect.close();
    statementSelect = null;
  }
  
  public void updateLogActionTableEscidocComponent() throws Exception {

    int offset = 0;
    boolean hasMoreResults = true;
    int resultIdAction = 0;
    String resultName = null;
    Pattern pattern = Pattern.compile("escidoc:\\d+", Pattern.CASE_INSENSITIVE);
//    Pattern pattern = Pattern.compile("escidoc:\\d+:?\\d?", Pattern.CASE_INSENSITIVE);
    Matcher matcher = null;
    PreparedStatement statementSelect = null;
    PreparedStatement statementUpdate = null;
    statementSelect = connection.prepareStatement(
        "SELECT idaction, name, type, url_prefix FROM piwik_log_action WHERE (name REGEXP '^(http://|https://)?(pubman\\\\.mpdl\\\\.mpg\\\\.de|pure\\\\.mpg\\\\.de).*/escidoc:(\\\\d)+:?(\\\\d)*/component+');");
//        "SELECT idaction, name, type, url_prefix FROM piwik_log_action WHERE (name REGEXP '^(http://|https://)?(pubman\\\\.mpdl\\\\.mpg\\\\.de|pure\\\\.mpg\\\\.de).*/escidoc:(\\\\d)+:?(\\\\d)*/component+') LIMIT ? OFFSET ?;");
//    while (hasMoreResults) {
//      statementSelect.setInt(1, SQL_LIMIT);
//      statementSelect.setInt(2, offset);
      System.out.println("Sending statement...");

      ResultSet result = statementSelect.executeQuery();
      System.out.println("Processing Result...");
      int count = 0;
      int resultUpdate = 0;


      statementUpdate = connection.prepareStatement(
          "UPDATE piwik_log_action SET name = ?, url_prefix = ?, type = ? WHERE idaction = ?;");
      while (result.next()) {
        resultIdAction = result.getInt("idaction");
        resultName = result.getString("name");
//        System.out.println(resultIdAction + " | " + resultName + " | " + result.getString("type"));
        matcher = pattern.matcher(resultName);
        String itemId = null;
        if (matcher.find()) {
          itemId = matcher.group();
//          System.out.println("Matched ID: " + itemId);
        }
        if (itemId != null) {
          String restOfPath = resultName.substring(resultName.indexOf("/component/") + 11); // substring after /component/
          String updatedName = null;
          if (restOfPath.contains("escidoc:") || restOfPath.contains("escidoc_")) {
            updatedName = PURE_ITEM_URL + (itemId.substring(itemId.indexOf(":") + 1)) + COMPONENT_PATH + "file_" + restOfPath.substring(restOfPath.indexOf("escidoc") + 8);
//            updatedName = PURE_ITEM_URL + (itemId.substring(itemId.indexOf(":") + 1)).replace(":", "_") + COMPONENT_PATH + "file_" + restOfPath.substring(restOfPath.indexOf("escidoc") + 8);
          } else {
            updatedName = PURE_ITEM_URL + (itemId.substring(itemId.indexOf(":") + 1)) + COMPONENT_PATH +  restOfPath;
//            updatedName = PURE_ITEM_URL + (itemId.substring(itemId.indexOf(":") + 1)).replace(":", "_") + COMPONENT_PATH +  restOfPath;
          }
          statementUpdate.setString(1, updatedName);
          statementUpdate.setInt(2, 2); // set url_prefix to 2 (https://)
          statementUpdate.setInt(3, 3); // set type to 3 (download)
          statementUpdate.setInt(4, resultIdAction);
//          System.out.println(
//              "Updating [" + resultIdAction + "] from " + resultName + " to " + updatedName);
          resultUpdate = statementUpdate.executeUpdate();
          if (resultUpdate == 1) {
            System.out.println("Finished Updating [" + resultIdAction + "] from " + resultName
                + " to " + updatedName);
          } else {
            System.out.println("Error updating [" + resultIdAction + "] from " + resultName + " to "
                + updatedName);
          }

        }
        count++;
      }
      if (count < SQL_LIMIT) {
        System.out.println("finishing loop");
        hasMoreResults = false;
      }
//      offset += count;
//      System.out.println("Offset: " + offset);
      System.out.println("Count : " + count);
//    }

    System.out.println("Closing Connection...");

    statementSelect.close();
    statementSelect = null;
  }

  

  protected void finalize() throws SQLException {
    try {
      connection.close();
      connection = null;
    } finally {

      // if (statement != null) {
      // try {
      // statement.close();
      // } catch (SQLException sqlex) {
      // }
      //
      // statement = null;
      // }

      if (connection != null) {
        try {
          connection.close();
        } catch (SQLException sqlex) {
        }

        connection = null;
      }
    }
  }
}
