package first;

import java.io.Serializable;

public class ValueWithDouble implements Serializable {

    private Integer inputValue;
    private Integer doubleValue;

    public ValueWithDouble(Integer inputValue) {
        this.inputValue = inputValue;
        this.doubleValue = inputValue*inputValue;
    }

    public Integer getInputValue() {
        return inputValue;
    }

    public void setInputValue(Integer inputValue) {
        this.inputValue = inputValue;
    }

    public Integer getDoubleValue() {
        return doubleValue;
    }

    public void setDoubleValue(Integer doubleValue) {
        this.doubleValue = doubleValue;
    }
}
