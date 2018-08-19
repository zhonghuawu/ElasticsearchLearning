package Utils;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import java.lang.reflect.Type;

/**
 * Desc:
 *
 * @author Huaa
 * @date 2018/8/19 16:16
 */

public class JsonUtil {

    private static Gson gson = new Gson();

    public static <T> T fromJson(String json) {
        return gson.fromJson(json, new TypeToken<T>() {
        }.getType());
    }

    public static <T> T fromJson(String json, Type typeOfT) {
        return gson.fromJson(json, typeOfT);
    }

    public static String toJson(Object object) {
        return gson.toJson(object);
    }

}
