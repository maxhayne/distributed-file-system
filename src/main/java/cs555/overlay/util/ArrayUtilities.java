package cs555.overlay.util;

import java.util.ArrayList;

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
        count++;
      }
    }
    String[] newArray = new String[array.length-count];
    int index = 0;
    for ( String string : array ) {
      if ( !java.util.Objects.equals( string, toRemove ) ) {
        newArray[index] = string;
        index++;
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
   * Helper function to convert an ArrayList<Integer> to an int[].
   *
   * @param list ArrayList<Integer> to convert to int[]
   * @return int[] converted from ArrayList
   */
  public static int[] arrayListToArray(ArrayList<Integer> list) {
    return list.stream().mapToInt( i -> i ).toArray();
  }

}
