package com.databricks.fsi.bpipe;

import org.scalatest.TagAnnotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@TagAnnotation
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD, ElementType.TYPE})
// The tag annotation must be written in Java, not Scala
// because annotations written in Scala are not accessible at runtime.
public @interface BPipeEnvironmentTest {
}
