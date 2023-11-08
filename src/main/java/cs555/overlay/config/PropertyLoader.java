package cs555.overlay.config;

import cs555.overlay.util.Logger;

import java.io.FileInputStream;
import java.util.Properties;

/**
 * Class to be used as a singleton in order to load the application.properties
 * file only once, and in one place. Properties can then be queried of the
 * properties object stored inside the singleton.
 *
 * @author hayne
 */
public class PropertyLoader {

  private static final Logger logger = Logger.getInstance();
  private static final PropertyLoader loader = new PropertyLoader();
  private final Properties properties;

  /**
   * Private Constructor. Attempts to load the property file which is stored at
   * the location specified by the static member 'propertyFile' in the
   * ApplicationProperties interface.
   */
  private PropertyLoader() {
    properties = new Properties();
    try ( FileInputStream in = new FileInputStream(
        ApplicationProperties.propertyFile ) ) {
      properties.load( in );
    } catch ( Exception e ) {
      logger.error(
          "'"+ApplicationProperties.propertyFile+"' file could not be loaded. "+
          e.getMessage() );
    }
  }

  /**
   * Returns the singleton instance of the PropertyLoader.
   *
   * @return PropertyLoader singleton instance
   */
  public static PropertyLoader getInstance() {
    return loader;
  }

  /**
   * Get a property stored in the properties member of the object. Basically
   * overloading the original function of the same name for the Properties
   * object. Doing this instead of making the member public.
   *
   * @param property To be gotten from properties object
   * @return property linked to in application.properties file
   */
  public String getProperty(String property) {
    return properties.getProperty( property );
  }

  /**
   * Get a property stored in the properties member of the object. Basically
   * overloading the original function of the same name for the Properties
   * object. Doing this instead of making the member public. If a value could
   * not be found for the key provided, the default value is returned instead.
   *
   * @param property To be gotten from properties object
   * @return property linked to in application.properties file
   */
  public String getProperty(String property, String defaultValue) {
    return properties.getProperty( property, defaultValue );
  }

}
