package com.databricks.fsi.bpipe;

import java.lang.annotation.*;
import org.scalatest.TagAnnotation;

@TagAnnotation
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD, ElementType.TYPE})
// The tag annotation must be written in Java, not Scala
// because annotations written in Scala are not accessible at runtime.
public @interface BPipeEnvironmentTest {}
