package org.HFC;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

public class HuffManEncoder implements Encoder{
    private Map<Character,Integer> characterFrequency;
    private Tree<Character,Integer> encodingTree;
    private Map<Character,String> encodings;
    private Map<String,Character> decodings;
    private final TreeBuildStrategy<Character,Integer> buildStrategy;

    private static final Logger logger = Logger.getLogger(HuffManEncoder.class.getName());

    public HuffManEncoder() {
        this.characterFrequency = new HashMap<>();
        this.buildStrategy = new HuffManTreeBuildStrategy<>();
    }

    @Override
    public void encode(String inputFilePath, String outputFilePath ) throws IOException{

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(outputFilePath))) {
            // Write content to the file
            for(String enc: this.decodings.keySet())
            {
                writer.write(enc+":"+(int) decodings.get(enc)+"\n");
            }
            writer.write("<<EOH>>\n");
            logger.info("Header has been written to the file.");
        } catch (IOException e) {
            logger.severe(e.getMessage());
            throw new IOException();
        }

        try(FileOutputStream fos = new FileOutputStream(outputFilePath,true)){
            try(BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(inputFilePath), StandardCharsets.UTF_8))){
                int currentCharacter;
                StringBuilder bitsArray = new StringBuilder();

                while((currentCharacter=reader.read())!=-1)
                {
                    bitsArray.append(this.encodings.get((char) currentCharacter));

                    if(bitsArray.length()>1024)
                    {
                        byte bytes[]= bitsToBytes(bitsArray.substring(0,1024));
                        bitsArray.delete(0, 1024);
                        fos.write(bytes);
                    }
                }
                fos.write(bitsToBytes(bitsArray.toString()));
            }catch (IOException e) {
                logger.severe(e.getMessage());
                throw new IOException();
            }

            logger.info("File Successfully encoded and written to destination: "+outputFilePath);
        } catch (IOException e) {
            logger.severe(e.getMessage());
            throw new IOException(e);
        }
    }

    public static byte[] bitsToBytes(String bitString) {
        int numBytes = (int) Math.ceil((double) bitString.length() / 8);
        byte[] bytes = new byte[numBytes];

        for (int i = 0; i < numBytes; i++) {
            int startIndex = i * 8;
            int endIndex = Math.min(startIndex + 8, bitString.length());
            String byteString = bitString.substring(startIndex, endIndex);
            bytes[i] = (byte) Integer.parseInt(byteString, 2);
        }
        return bytes;
    }

    @Override
    public void fit(String inputFilePath) throws IOException,IllegalArgumentException{

        try(BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(inputFilePath), StandardCharsets.UTF_8))){
            int currentCharacter;
            while((currentCharacter =reader.read())!=-1) {
                this.characterFrequency.put((char) currentCharacter,this.characterFrequency.getOrDefault((char) currentCharacter,0)+1);
            }
        }catch(IOException e)
        {
            throw new IOException();
        }

        //Building tree with strategy and creating tree object which will have root node
        this.encodingTree= new HuffManTree<>(this.buildStrategy.BuildTree(this.characterFrequency));
        logger.info("Encoding tree Generated");

        this.encodings=this.encodingTree.getEncodings();
        this.decodings= new HashMap<>();
        for(Character curr: this.encodings.keySet()) {
            this.decodings.put(this.encodings.get(curr),curr);
        }

        logger.info("Encoding fit complete");
    }
}
