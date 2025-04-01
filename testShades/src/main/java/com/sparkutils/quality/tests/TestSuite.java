package com.sparkutils.quality.tests;

import org.junit.*;
import org.junit.runner.*;
import org.junit.internal.TextListener;
import com.sparkutils.qualityTests.*;
import com.sparkutils.quality.tests.*;
import com.sparkutils.qualityTests.bloom.*;
import com.sparkutils.qualityTests.id.*;
import com.sparkutils.qualityTests.mapLookup.*;
import static com.sparkutils.qualityTests.TestUtilsEnvironment.setupDefaultsViaCurrentSession;

public class TestSuite {

    public static void runTests() {
        // use the active session to setup defaults if present.
        setupDefaultsViaCurrentSession();

        JUnitCore junit = new JUnitCore();
        junit.addListener(new TextListener(System.out) {
            long start = 0;

            @Override
            public void testStarted(Description description){
                start = System.currentTimeMillis();
                System.out.print("Running: " + description.getDisplayName());
            }

            @Override
            public void testFinished(Description description){
                System.out.println(", finished in: " + ((System.currentTimeMillis() - start)/1000 ) + "s");
            }

        });

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
                YamlTests.class,
                SubExpressionEliminationTest.class,
                VersionSerializingTest.class
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

    /**
     * Allow for running individual tests from DBRs using a higher scalatest
     * @param cname
     */
    public static void runClass(String cname) throws java.lang.ClassNotFoundException {
        JUnitCore junit = new JUnitCore();
        junit.addListener(new TextListener(System.out));

        Result result = junit.run( TestSuite.class.getClassLoader().loadClass(cname) );
        resultReport(result);
    }
}
