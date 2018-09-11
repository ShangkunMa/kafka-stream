package kafka.example.stream.model;

import com.google.gson.Gson;
import kafka.example.stream.util.GeneralHelper;
import kafka.example.stream.util.LogHelper;

public class Data {

    private Long time;
    private Integer id;
    private Integer type;
    private String name;
    private Integer amount;
    private Integer saveCount = 0;

    public static Data fromStr(String str) {
        LogHelper.info("Data.fromStr got str: " + str);
        return new Gson().fromJson(str, Data.class);
    }

    @Override
    public String toString() {
        return new Gson().toJson(this);
    }

    public boolean isValid() {
        if (saveCount < 10) {
            return GeneralHelper.isTimeInDruidWindow(time);
        }
        LogHelper.info("data is invalid");
        return false;
    }

    public Long getTime() {
        return time;
    }

    public void setTime(Long time) {
        this.time = time;
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getType() {
        return type;
    }

    public void setType(Integer type) {
        this.type = type;
    }

    public Integer getAmount() {
        return amount;
    }

    public void setAmount(Integer amount) {
        this.amount = amount;
    }

    public int getSaveCount() {
        return saveCount;
    }

    public void addSaveCount()
    {
        this.saveCount++;
    }
}
