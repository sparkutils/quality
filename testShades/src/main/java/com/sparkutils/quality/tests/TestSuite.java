package com.sparkutils.quality.tests;

import org.junit.*;
import org.junit.runner.*;
import org.junit.internal.TextListener;
import com.sparkutils.qualityTests.*;
import com.sparkutils.quality.tests.*;
import com.sparkutils.qualityTests.bloom.*;
import com.sparkutils.qualityTests.id.*;
import com.sparkutils.qualityTests.mapLookup.*;

public class TestSuite {
    public static void runTests() {

        JUnitCore junit = new JUnitCore();
        junit.addListener(new TextListener(System.out));

        Result result = junit.run(
                JoinValidationTest.class,
                RoundTripTest3.class,
                MetaRuleSetTest.class,
                RoundTripPrivateTest.class,
                BloomTests.class,
                IDTests.class,
                MapLookupTests.class,
                AggregatesTest.class,
                BaseFunctionalityTest.class,
                CodeGenTest.class,
                DocMarkdownTest.class,
                DocsParserTest.class,
                LookupIdentificationTest.class,
                ReplaceWithMissingAttributesTest.class,
                RngAndRowIdTest.class,
                RoundTripTest.class,
                RuleEngineTest.class,
                RuleFolderTest.class,
                TrEitherTest.class,
                UserLambdaFunctionTest.class,
                ValidationTest.class,
                VariableIdentificationTest.class,
                UserLambdaFunctionCompilationTest.class,
                ExtensionParquetTest.class,
                ExtensionDeltaTest.class,
                ViewLoaderTest.class,
                MapLoaderTest.class,
                BloomLoaderTest.class,
                YamlTests.class
        );

        resultReport(result);
    }

    public static void resultReport(Result result) {
        System.out.println("Finished. Result: Failures: " +
                result.getFailureCount() + ". Ignored: " +
                result.getIgnoreCount() + ". Tests run: " +
                result.getRunCount() + ". Time: " +
                result.getRunTime() + "ms.");
    }
}
