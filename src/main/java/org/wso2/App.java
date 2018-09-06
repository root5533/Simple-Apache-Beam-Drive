package org.wso2;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;

import javax.xml.soap.Text;

public class App
{

    private static class ComputeWordLength extends DoFn<String, Integer> {

        @ProcessElement
        public void processElement(@Element String word, OutputReceiver<Integer> out) {
            System.out.println("Word is >>>>>>>>>>>>>>>>>>>>>>>>>> " + word);
            out.output(word.length());
        }

    }

    private static class IntToString extends DoFn<Integer, String> {

        @ProcessElement
        public void processElement(@Element Integer number, OutputReceiver<String> out) {
            out.output(Integer.toString(number));
        }

    }

    public static void main( String[] args )
    {
        System.out.println( "Hello World!" );

        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline pipe = Pipeline.create(options);

        PCollection<String> lines = pipe.apply("Read input text", TextIO.read().from("input.txt"));

        PCollection<Integer> lengthCount = lines.apply(ParDo.of(new ComputeWordLength()));

        lengthCount.apply(ParDo.of(new IntToString()))
                .apply("Writing output text", TextIO.write().to("output.txt"));

        pipe.run();

    }
}
