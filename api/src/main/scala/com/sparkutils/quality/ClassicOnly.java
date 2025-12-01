package com.sparkutils.quality;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/**
 * Signifies that this function only works on classic SparkSession
 */
@Retention(RetentionPolicy.RUNTIME)
public @interface ClassicOnly {
}
