package tools.mpdl.mpg.de.matomo_database_tool;

/**
 * Hello world!
 *
 */
public class MatomoDbToolMain {

  public static void main(String[] args) throws Exception {
    if (args != null && args.length == 3) {
      MysqlDbController mysqlDbController = new MysqlDbController(args[0], args[1], args[2]);
      mysqlDbController.updateLogActionTableItemComponent();
      mysqlDbController.updateLogActionTableEscidocComponent();
      mysqlDbController.updateLogActionTableItem();
      mysqlDbController.updateLogActionTableEsciDoc();
      
    } else if (args != null && args.length == 1) {
      if ("--help".equals(args[0]) || "-man".equals(args[0])) {
        printHelp();
      } else {
        MysqlDbController mysqlDbController = new MysqlDbController(args[0]);
        mysqlDbController.updateLogActionTableItemComponent();
        mysqlDbController.updateLogActionTableEscidocComponent();
        mysqlDbController.updateLogActionTableItem();
        mysqlDbController.updateLogActionTableEsciDoc();
      }
    }
    else {
      printHelp();
    }
    System.out.println("------------------");
    System.out.println("===== FINISHED =====");
    System.out.println("------------------");
  }

  

  private static void printHelp() {
    System.out.println("----- MANUAL -----");
    System.out.println("MatomoDbMain [userName] [userPassword] [dbUrl]");
    System.out.println("OR");
    System.out.println("MatomoDbMain [dbUrl]");
    System.out.println(" - - - - - - - - -");
    System.out.println("MatomoDbMain franz 1234 localhost:3306/matomo");
    System.out.println("OR");
    System.out.println("MatomoDbMain localhost:3306/matomo");
    System.out.println("------------------");
  }
}
