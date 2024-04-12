package cs555.overlay.util;

import cs555.overlay.config.ApplicationProperties;

/**
 * Generic, thread-safe logging.
 *
 * @author stock
 * @author hayne
 */
public class Logger {

  private static final Logger logger = new Logger();

  /**
   * Private Constructor.
   */
  private Logger() {
  }

  /**
   * Get instance of singleton Logger
   *
   * @return logger singleton
   */
  public static Logger getInstance() {
    return logger;
  }

  /**
   * Print DEBUG log message.
   *
   * @param message to append to log
   */
  public void debug(String message) {
    if (!ApplicationProperties.logLevel.equalsIgnoreCase("debug")) {
      return;
    }
    String[] details = details();
    System.out.printf("%-7s %s %s %s%n", "[DEBUG]", details[0], details[1],
        message);
  }

  /**
   * Print INFO log message.
   *
   * @param message to append to log
   */
  public void info(String message) {
    String[] details = details();
    System.out.printf("%-7s %s %s %s%n", "[INFO]", details[0], details[1],
        message);
  }

  /**
   * Print ERROR log message.
   *
   * @param message to append to log
   */
  public void error(String message) {
    String[] details = details();
    System.err.printf("%-7s %s %s %s%n", "[ERROR]", details[0], details[1],
        message);
  }

  /**
   * Get details about the location at which DEBUG, INFO, or ERROR were called
   * from. The class, the method, and the line number.
   *
   * @return String[] of location info about error
   */
  private String[] details() {
    String[] details = new String[3];
    details[0] = Thread.currentThread().getStackTrace()[3].getClassName();
    details[1] = Thread.currentThread().getStackTrace()[3].getMethodName();
    details[2] = Integer.toString(
        Thread.currentThread().getStackTrace()[3].getLineNumber());
    return details;
  }
}
