/**
 * Autogenerated by Thrift Compiler (0.17.0)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package com.google.edwmigration.dumper.ext.hive.metastore.thrift.api.superset;


@javax.annotation.Generated(value = "Autogenerated by Thrift Compiler (0.17.0)", date = "2023-08-11")
public enum QueryState implements org.apache.thrift.TEnum {
  INITED(0),
  EXECUTING(1),
  FAILED(2),
  FINISHED(3),
  TIMED_OUT(4);

  private final int value;

  private QueryState(int value) {
    this.value = value;
  }

  /**
   * Get the integer value of this enum value, as defined in the Thrift IDL.
   */
  @Override
  public int getValue() {
    return value;
  }

  /**
   * Find a the enum type by its integer value, as defined in the Thrift IDL.
   * @return null if the value is not found.
   */
  @org.apache.thrift.annotation.Nullable
  public static QueryState findByValue(int value) { 
    switch (value) {
      case 0:
        return INITED;
      case 1:
        return EXECUTING;
      case 2:
        return FAILED;
      case 3:
        return FINISHED;
      case 4:
        return TIMED_OUT;
      default:
        return null;
    }
  }
}