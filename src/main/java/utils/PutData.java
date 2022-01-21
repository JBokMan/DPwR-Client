package utils;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;

@AllArgsConstructor
@Data
public class PutData implements Serializable {
    private Integer dataSize;
    private String dataAddress;

    public String toString() {
        String[] temp = dataAddress.split("\s");
        return dataSize.toString() + "," + temp[1];
    }
}
