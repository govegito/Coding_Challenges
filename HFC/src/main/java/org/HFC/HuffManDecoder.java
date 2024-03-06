package org.HFC;

import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

public class HuffManDecoder implements Decoder {

    private Map<String,Character> decodings;
    public HuffManDecoder() {
        this.decodings = new HashMap<>();
    }

    private static final Logger logger = Logger.getLogger(HuffManDecoder.class.getName());

    @Override
    public void decode(String inputFilePath, String outputFilePath) throws IOException{

        try(FileInputStream reader = new FileInputStream(inputFilePath)){
            int currentCharacter;
            String line;
            List<Character> currentLine = new ArrayList<>();
            while((currentCharacter= reader.read())!=-1)
            {
                if((char)currentCharacter=='\n')
                {
                    StringBuilder stringBuilder = new StringBuilder();
                    for (char c : currentLine) {
                        stringBuilder.append(c);
                    }
                    String currentLineString = stringBuilder.toString();
                    String codes[]= currentLineString.split(":");
                    if(currentLineString.equalsIgnoreCase("<<EOH>>"))
                        break;
                    this.decodings.put(codes[0],(char)Integer.parseInt(codes[1]));

                    currentLine=new ArrayList<>();
                }
                else
                    currentLine.add((char) currentCharacter);
            }
            StringBuilder bytesToBitsString= new StringBuilder();
            try (BufferedWriter writer = new BufferedWriter(new FileWriter(outputFilePath))) {
                while((currentCharacter= reader.read())!=-1)
                {
                    for(int i=7;i>=0;i--)
                    {
                        if(((currentCharacter>>i)&1)==1)
                            bytesToBitsString.append("1");
                        else
                            bytesToBitsString.append("0");

                        if(this.decodings.containsKey(bytesToBitsString.toString()))
                        {
                            writer.write(this.decodings.get(bytesToBitsString.toString()));
                            bytesToBitsString.delete(0,bytesToBitsString.length());
                        }
                    }
                }

            }catch (IOException e)
            {
                logger.severe(e.getMessage());
                throw new IOException();
            }

        } catch (IOException e) {
            throw new IOException(e);
        }
        logger.info("Decoding Completed");

    }

}
