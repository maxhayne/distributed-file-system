package cs555.overlay.config;

/**
 * Interface to store properties as loaded from the application.properties file.
 * The singleton PropertyLoader is created, loads the properties stored in the
 * properties file, and then its method getProperty( String ) are used
 */
public interface ApplicationProperties {
  String propertyFile =
      System.getProperty( "user.dir" )+"/config/application.properties";

  String controllerHost =
      PropertyLoader.getInstance().getProperty( "controllerHost", "localhost" );

  int controllerPort = Integer.parseInt(
      PropertyLoader.getInstance().getProperty( "controllerPort", "46529" ) );

  String storageType = PropertyLoader.getInstance()
                                     .getProperty( "storageType",
                                         "replication" )
                                     .toLowerCase();

  String logLevel = PropertyLoader.getInstance()
                                  .getProperty( "logLevel", "info" )
                                  .toLowerCase();
}
