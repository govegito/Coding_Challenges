package org.HFC;

import java.io.IOException;

public interface Decoder {

    public void decode(String inputFilePath, String outputFilePath) throws IOException;

}
