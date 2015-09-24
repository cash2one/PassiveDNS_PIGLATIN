# Summary
This is a PIG LATIN script that analyse (anonymized) passive DNS data collected by the Dutch Academic Network (SURFNet). Such Passive DNS was collected based on a list of websites that offer DDoS as a paid service, aka Booters, DDoS-for-Hire, and Stressers.

## Data Meta-Description
* ~7 columns (Depending on recordType): timestamp, recordType, srcIpAnon, In, answer, booterInformation, error;

#### recordType can be the following: 
- Q(Q) is the original query;
- Q(R) is the query in the answer;
- R(ANS) is the answer;
- R(AUT) is authority;
- R(ADD) is additional;

#### Example:
-- 1434735481, Q(Q), c861aaa8307395e94c0bc1d88e9846ff168071252198640801b108219b3899be, IN, A, [Booter domain name]
-- 1434735481, Q(R), c861aaa8307395e94c0bc1d88e9846ff168071252198640801b108219b3899be, IN, A, [Booter domain name], NOERROR
-- 1434735481, R(ANS), c861aaa8307395e94c0bc1d88e9846ff168071252198640801b108219b3899be, IN, A, [Booter IP Address]

#### Note: 1 Q(Q) => 1 Q(R) && one or many R(ANS)
#### There are blank lines!!!

#### We can provide the data upon request.
