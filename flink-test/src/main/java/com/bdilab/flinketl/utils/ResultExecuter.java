package com.bdilab.flinketl.utils;

import com.bdilab.flinketl.utils.exception.*;
import org.apache.log4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;

/**
 * Author: cyz
 * Date: 2019/10/16
 * Description: 业务层代码执行器
 */
public abstract class ResultExecuter<T> {

    private Logger logger = Logger.getLogger(ResultExecuter.class);
    private org.slf4j.Logger logger1 = LoggerFactory.getLogger(ResultExecuter.class);
    public abstract T run() throws Exception;

    public GlobalResultUtil<T> execute(){
        GlobalResultUtil<T> resultUtil = null;
        try {
            T data = run();
            resultUtil = GlobalResultUtil.ok(data);
        }catch (DatabaseNotExistException e){
            e.printStackTrace();
            logger.error("请求数据库不存在");
            logger.error(e.toString());
            resultUtil = GlobalResultUtil.inexistence(e.getMessage());
        }catch (InfoNotInDatabaseException e){
            e.printStackTrace();
            logger.error("请求信息不存在");
            logger.error(e.toString());
            resultUtil = GlobalResultUtil.inexistence(e.getMessage());
        }catch (TableNotExistException e){
            e.printStackTrace();
            logger.error("请求表不存在");
            logger.error(e.toString());
            resultUtil = GlobalResultUtil.inexistence(e.getMessage());
        }catch (SQLException e){
            e.printStackTrace();
            logger.error("请求数据库配置错误");
            logger.error(e.toString());
            resultUtil = GlobalResultUtil.databaseErrorException(e.getMessage(),null);
        }catch (IllegalArgumentException e){
            e.printStackTrace();
            logger.error("请求参数错误");
            logger.error(e.toString());
            resultUtil = GlobalResultUtil.illegalArguments(e.getMessage());
        } catch (ElementNotExistException e){
            e.printStackTrace();
            logger.error("请求任务配置错误");
            logger.error(e.toString());
            resultUtil = GlobalResultUtil.illegalArguments(e.getMessage());
        }catch (AbnormalOperationException e){
            e.printStackTrace();
            logger.error("用户操作错误！");
            logger.error(e.toString());
            resultUtil = GlobalResultUtil.abnormalOperation(e.getMessage());
        } catch (SqlAntiInjectionException e){
            e.printStackTrace();
            logger.error("sql防注入！");
            logger.error(e.toString());
            resultUtil = GlobalResultUtil.abnormalOperation(e.getMessage());
        }catch (IndexOutOfBoundsException e){
            e.printStackTrace();
            logger.error(e.getMessage());
            logger.error(e.toString());
            resultUtil = GlobalResultUtil.abnormalOperation(e.getMessage());
        } catch (Throwable e){
            e.printStackTrace();
            logger.error("运行出错");
            logger.error(e.toString());
            resultUtil = GlobalResultUtil.errorMsg("运行出错");
        }
        return resultUtil;
    }
}

