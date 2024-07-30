/*
 *@Type SetCommand.java
 * @Desc
 * @Author urmsone urmsone@163.com
 * @date 2024/6/13 01:59
 * @version
 */
package model.command;

import dto.RespDTO;
import dto.RespStatusTypeEnum;
import lombok.Getter;
import lombok.Setter;
import service.Store;

@Setter
@Getter
public class SetCommand extends AbstractCommand {
    private String key;

    private String value;

    public SetCommand(String key, String value) {
        super(CommandTypeEnum.SET);
        this.key = key;
        this.value = value;
    }
    public RespDTO getRespDTO(Store store){
        String resvalue;
        RespDTO resp;
        if(this.value==null){
            resvalue= store.get(key);
            resp = new RespDTO(RespStatusTypeEnum.SUCCESS, resvalue);
        }else{
            store.set(key,value);
            resp = new RespDTO(RespStatusTypeEnum.SUCCESS, "新增成功");
        }
        return resp;
    }
}
