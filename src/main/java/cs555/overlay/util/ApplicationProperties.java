package cs555.overlay.util;

/**
 * Interface to store properties as loaded from the
 * application.properties file. The singleton PropertyLoader
 * is created, loads the properties stored in the properties
 * file, and then its method getProperty( String ) are used
 */
public interface ApplicationProperties {
    public static final String propertyFile = System.getProperty("user.dir") + "/config/application.properties";
    
    public static final String controllerHost = PropertyLoader.getInstance()
            .getProperty( "controllerHost", "localhost" );
    
    public static final int controllerPort = Integer.parseInt( PropertyLoader.getInstance()
            .getProperty( "controllerPort", "46529" ) );
    
    public static final String storageType = PropertyLoader.getInstance()
            .getProperty( "storageType", "replication" );
}
