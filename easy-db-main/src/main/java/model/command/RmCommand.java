/*
 *@Type RmCommand.java
 * @Desc
 * @Author urmsone urmsone@163.com
 * @date 2024/6/13 01:57
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
public class RmCommand extends AbstractCommand {
    private String key;
    public RmCommand(String key) {
        super(CommandTypeEnum.RM);
        this.key = key;
    }
    public RespDTO getRespDTO(Store store){
        RespDTO resp;
        store.rm(this.key);
        resp = new RespDTO(RespStatusTypeEnum.SUCCESS, "删除成功");
        return resp;
    }

}
