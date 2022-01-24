import org.junit.Test;

import java.io.File;

import static org.junit.Assert.*;

public class WordCount$Test {


    @Test
    public void debug() throws Exception {
        String[] input = new String[2];

        /*
        1. put the data.txt into a folder in your pc
        2. add the path for the following two files.
            windows : update the path like "file:///C:/Users/.../projectDirectory/data.txt"
            mac or linux: update the path like "file:///Users/.../projectDirectory/data.txt"
        */

//        File f = new File("./data.txt");
//        boolean b = f.exists();
//        int i = 0;
        input[0] = "file:///./data.txt";
        input[1] = "file:///./output.txt";

        WordCount wc = new WordCount();
        wc.debug(input);
    }
}