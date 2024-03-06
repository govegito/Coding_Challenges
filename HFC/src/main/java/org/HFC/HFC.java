package org.HFC;

import org.apache.commons.cli.*;

import java.io.IOException;
import java.util.logging.Logger;

public class HFC {
    private static final Logger logger = Logger.getLogger(HFC.class.getName());

    public static void main(String[] args) {

        Options options = new Options();
        options.addOption("e", "encode", false, "Encode mode");
        options.addOption("d", "decode", false, "Decode mode");
        options.addOption("t", "test", false, "Test mode");
        options.addOption("i", "input", true, "Input file path");
        options.addOption("o", "output", true, "Output file path");
        options.addOption("f", "final", true, "Final output file path");

        CommandLineParser parser = new DefaultParser();
        try {
            CommandLine cmd = parser.parse(options, args);

            if (cmd.hasOption("e")) {
                String inputFilePath = cmd.getOptionValue("i");
                String outputFilePath = cmd.getOptionValue("o");
                encode(inputFilePath, outputFilePath);
            }

            if (cmd.hasOption("d")) {
                String inputFilePath = cmd.getOptionValue("i");
                String outputFilePath = cmd.getOptionValue("o");
                decode(inputFilePath, outputFilePath);
            }

            if (cmd.hasOption("t")) {
                String inputFilePath = cmd.getOptionValue("i");
                String outputFilePath = cmd.getOptionValue("o");
                String finalOutputFilePath = cmd.getOptionValue("f");
                encode(inputFilePath, outputFilePath);
                decode(outputFilePath, finalOutputFilePath);
            }
        } catch (ParseException e) {
            System.err.println("Error parsing command line options: " + e.getMessage());
        }
    }

    private static void encode(String inputFilePath, String outputFilePath) {
        Encoder encoder = new HuffManEncoder();
        try {
            encoder.fit(inputFilePath);
            encoder.encode(inputFilePath, outputFilePath);
        } catch (IOException e) {
            logger.info("Encoding failed");
        }
    }

    private static void decode(String inputFilePath, String outputFilePath) {
        Decoder decoder = new HuffManDecoder();
        try {
            decoder.decode(inputFilePath, outputFilePath);
        }catch (IOException e)
        {
            logger.info("Decoding failed");
        }
    }
}
