package org.HFC;

import java.io.IOException;

public interface Encoder {

    public void encode(String inputFilePath, String outputFilePath) throws IOException;
    public void fit(String inputFilePath) throws IOException,IllegalArgumentException;

}
