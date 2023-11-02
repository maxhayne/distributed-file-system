package cs555.overlay.util;

import java.util.ArrayList;
import java.util.Arrays;

/**
 * Stores some useful functions for working with arrays.
 *
 * @author hayne
 */
public class ArrayUtilities {

  /**
   * Helper function to remove elements from a String array and return a new
   * array resized accordingly.
   *
   * @param array to remove elements from
   * @param toRemove the element to remove from array
   * @return a new array with all instances of toRemove removed
   */
  public static String[] removeFromArray(String[] array, String toRemove) {
    if ( array == null ) {
      return null;
    }
    int count = 0;
    for ( String string : array ) {
      if ( java.util.Objects.equals( string, toRemove ) ) {
        ++count;
      }
    }
    String[] newArray = new String[array.length-count];
    int index = 0;
    for ( String string : array ) {
      if ( !java.util.Objects.equals( string, toRemove ) ) {
        newArray[index] = string;
        ++index;
      }
    }
    return newArray;
  }

  /**
   * Helper function to set any instance of one value in a String array to
   * another. This cannot be used with an array of primitives, unfortunately.
   *
   * @param array array to replace values of
   * @param toReplace instances of this will be changed
   * @param setTo all instances of toReplace will be replaced with setTo
   */
  public static <T> void replaceArrayItem(T[] array, T toReplace, T setTo) {
    for ( int i = 0; i < array.length; ++i ) {
      if ( java.util.Objects.equals( array[i], toReplace ) ) {
        array[i] = setTo;
      }
    }
  }

  /**
   * Same as replaceArrayItem, but only replaces the first instance of the item
   * setTo.
   *
   * @param array to search for value toReplace
   * @param toReplace the item to replace
   * @param setTo the item to replace it with
   */
  public static <T> void replaceFirst(T[] array, T toReplace, T setTo) {
    for ( int i = 0; i < array.length; ++i ) {
      if ( java.util.Objects.equals( array[i], toReplace ) ) {
        array[i] = setTo;
        break;
      }
    }
  }

  /**
   * Helper function to convert an ArrayList<Integer> to an int[].
   *
   * @param list ArrayList<Integer> to convert to int[]
   * @return int[] converted from ArrayList
   */
  public static int[] arrayListToArray(ArrayList<Integer> list) {
    return list.stream().mapToInt( i -> i ).toArray();
  }

  /**
   * Helper function to check if an int[] contains a specific int. If supplied
   * int[] is null, will always return true.
   *
   * @param value the array is being checked for
   * @param array being checked
   * @return true if array contains value, false otherwise
   */
  public static boolean contains(int[] array, int value) {
    if ( array == null ) {
      return false;
    }
    for ( int i : array ) {
      if ( i == value ) {
        return true;
      }
    }
    return false;
  }

  /**
   * Checks if an array contains a specific element. If it does, returns the
   * index of the first occurrence of that element. Otherwise, returns -1.
   *
   * @param array to be checked
   * @param value to check the array for
   * @return index of first occurrence of value, -1 if no occurrences
   */
  public static int contains(String[] array, String value) {
    if ( array == null ) {
      return -1;
    }
    for ( int i = 0; i < array.length; ++i ) {
      if ( java.util.Objects.equals( array[i], value ) ) {
        return i;
      }
    }
    return -1;
  }

  /**
   * Counts the number of null pointers unset pointers in a byte[][].
   *
   * @param array to check
   * @return number of nulls in array
   */
  public static int countNulls(byte[][] array) {
    if ( array == null ) {
      return 0;
    }
    int count = 0;
    for ( byte[] b : array ) {
      if ( Arrays.equals( b, null ) ) {
        ++count;
      }
    }
    return count;
  }

}
