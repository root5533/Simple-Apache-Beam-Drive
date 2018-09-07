package org.wso2;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

public class WordCount {

    private interface WordCountOptions extends PipelineOptions {

        @Description("Set input target")
        @Default.String("input.txt")
        String getInputFile();
        void setInputFile(String value);

        @Description("Set output target")
        @Default.String("output")
        String getOutput();
        void setOutput(String value);

    }

    private static class ExtractWordsFn extends DoFn<String, String> {

        @ProcessElement
        public void processElement(@Element String element, OutputReceiver<String> out) {

            String[] words = element.split(" ");
            for (String word : words) {
                if (!word.trim().isEmpty()) {
                    out.output(word);
                }
            }
        }

    }

    private static class CountWords extends PTransform<PCollection<String>, PCollection<KV<String, Long>>> {

        public PCollection<KV<String, Long>> expand(PCollection<String> lines) {

            PCollection<String> words = lines.apply(ParDo.of(new ExtractWordsFn()));
//            PCollection<String> filter = words.apply(Filter.by((String word) -> !word.isEmpty()));
            PCollection<KV<String, Long>> wordCounts = words.apply(Count.perElement());
            return wordCounts;

        }

    }

    public static class FormatAsTextFn extends SimpleFunction<KV<String, Long>, String> {

        @Override
        public String apply(KV<String, Long> input) {
            return input.getKey() + " : " + input.getValue();
        }

    }

    private static void runWordCount(WordCountOptions options) {

        String delimiter = " ";

        Pipeline pipe = Pipeline.create(options);
        pipe.apply("ReadFile", TextIO.read().from(options.getInputFile()))
                .apply(new CountWords())
                .apply(MapElements.via(new FormatAsTextFn()))
                .apply("Final Result", TextIO.write().to("Final"));

        pipe.run().waitUntilFinish();

    }


    public static void main(String args[]) {

        WordCountOptions options = PipelineOptionsFactory.fromArgs(args).as(WordCountOptions.class);
        runWordCount(options);

    }

}
