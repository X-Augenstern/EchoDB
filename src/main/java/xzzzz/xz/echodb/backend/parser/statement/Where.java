package xzzzz.xz.echodb.backend.parser.statement;

/**
 * 目前 EchoDB 的 Where 只支持两个条件的与和或。例如有条件的 Delete，计算 Where，最终就需要获取到条件范围内所有的 UID。EchoDB 只支持已索引字段作为 Where 的条件
 */
public class Where {

    public SingleExpression singleExp1;

    public String logicOp;

    public SingleExpression singleExp2;
}
