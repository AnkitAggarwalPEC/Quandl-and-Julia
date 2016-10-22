

#securityId :
# year : "Year"
#data : [{ priority:'1', source: {name:'quandl', id:'quandldataid' }, columns:[x,y,,....z], data:[[]]},
#           { priority:'2', source: {name:'quandl', id:'quandldataid2' }, columns:[x,y,,....z], data:[[]]}

#Before this is executed the we need to generate the Unique ID


function createsecurityid(exchange :: UTF8String ,Ticker :: UTF8String ,securityType :: UTF8String)
    
end

"""
Just some helper functions starts
"""
function getsecurityid(symbol ::Any , frequency :: Int , exchange ::AbstractString , dataType ::UTF8String)
    
    mongoQuery = Dict("ticker" => symbol , "dataSources.frequency" => frequency ,
                        "exchange" => exchange , "dataSources.dataType" => dataType)
    doc = find(securityCollection , mongoQuery)
    return get(doc, "securityID", "NULL")    
end

function getdataarray(securityID :: AbstractString, year::Int ; priority :: Int = 1)
    dataDoc = find(dataCollection , query("securityID" => securityID ,
                                          "Year" => year,
                                          "data.priority" => priority
                                        )
                )
    dataColoumns = dataDoc["data"]["column"]
    actualData = dataDoc["data"]["column"]["data"]
    return dataColoumns , actualData
end
"""
Just some helper functions ends
"""



#function to gethistory on the basis of two dates

function gethistory(sDate :: AbstractString ,#    Here start date  is greater than end date --- Think of reverse nature of backtester
                    eDate:: AbstractString,
                    dataType::UTF8String,
                    symbols::Array{Any,1},
                    frequency::Int;
                    priority::Int = 1,
                    exchange::AbstractString = "NSE"
                    )

    for symbol in symbols

        securityID = getsecurityid(symbol , frequency , exchange , dataType)
        if securityID == "NULL"
            println("Data not found")
        else
            dataColoumns,actualData = get(securityID , Int(Dates.Year(DateTime(sDate))) , priority)
            if( addData(securityID,dataColoumns , actualData ,date , horizon) == -1 )
                #create Error over here
            else
                println("Data for the symbol " * symbol * " successfully stored")
            end
        end
    end
    
end

function gethistory(sDate :: AbstractString ,
                    horizon:: Int,
                    dataType::UTF8String,
                    symbols::Array{Any , 1},
                    frequency::Int;
                    priority::Int = 1
                    )
    for symbol in symbols

        securityID = getsecurityid(symbol , frequency , exchange , dataType)
        if securityID == "NULL"
            println("Data not found")
        else
            dataColoumns,actualData = get(securityID , Int(Dates.Year(DateTime(sDate))) , priority)
            if( addData(securityID,dataColoumns , actualData ,date , horizon) == -1 )
                #create Error over here
            else
                println("Data for the symbol " * symbol * " successfully stored")
            end
        end
    end
end

function gethistorybasedonsymbolids(sDate :: AbstractString ,
                                    eDate:: AbstractString,
                                    dataType::UTF8String,
                                    symbolIds::Array{Any , 1},
                                    frequency::Int;
                                    priority::Int = 1
                                    )
    for symbolId in symbolIds

        dataColoumns,actualData = get(securityID , Int(Dates.Year(DateTime(sDate))) , priority)
        if( addData(securityID,dataColoumns , actualData ,sdate , eDate) == -1 )
            #create Error over here
        else
            println("Data for the symbol " * symbol * " successfully stored")
        end
    end
end

function gethistorybasedonsymbolids(sDate :: AbstractString ,
                                    horizon:: Int,
                                    dataType::UTF8String,
                                    symbolIds::Array{Any , 1},
                                    frequency::Int;
                                    priority::Int = 1
                                    )
    for symbolId in symbolIds

        dataColoumns,actualData = get(securityID , Int(Dates.Year(DateTime(sDate))) , priority)
        if( addData( securityID,dataColoumns , actualData ,sDate , horizon) == -1 )
            #create Error over here
        else
            println("Data for the symbol " * symbol * " successfully stored")
        end
    end

end

function getindexofdate(dataColoumns :: Array{Any , 1})
    
    for i = 1: length(dataColoumns)
        if(dataColoumns[i] == "Date")
            return i
        end
    end
    return -1 
end

function addData(securityID :: AbstractString ,
                 dataColoumns :: Array{Any,1},
                 actualData :: Array{Any , 1},
                 sDate :: AbstractString,
                 eDate:: AbstractString)
    sDateObject = DateTime(sDate)
    eDateObject = DateTime(eDate)
    yearDiff = Int(Dates.Year(sDateObject) - Dates.Year(eDateObject))

    dateIndex = getindexofdate(dataColoumns)
    
    dataToReturn = Array{Any ,1}()
    if yearDiff == 0
        length = length(actualData)
        for i = 1 : length
            presentDate = actualData[i][dateIndex]
            presentDate = DateTime(presentDate)
            if (presentDate <= sDate && presentDate >= eDate)
                append!(dataToReturn , actualData[i])
            end
        end
    else
        #need to fetch multiple year --- difficult implemenation --- need to figure out
        append!(dataToReturn , actualData)
        dateObject = sDateObject - Dates.Year(1)
        yearDiff = yearDiff - 1;
        while yearDiff > 0
            dataColoumnNew , actualDataNew = getdataarray(securityID, Int(Dates.Year(DateTime(dateObject))) , 1)
            append!(dataToReturn , actualDataNew)
            yearDiff = yearDiff - 1;
            dateObject = dateObject - Dates.Year(1)
        end
        yearTobeFeatchedInt = Int(Dates.Year(dateObject)) - 1
        lastDate = string(yearTobeFeatchedInt)*"-12-31"
        dataColoumn , actualData = getdataarray( securityID, yearTobeFeatchedInt)
        dateIndex = getindexofdate(dataColoumn)
        for i = 1 : length(actualData)
            presentDateObject = DateTime(actualData[i][dateIndex])
            if (presentDateObject >= eDate)
                append!(dataToReturn , actualData[i])
            end
        end 
    end
    return  dataToReturn
end

function addData(securityID :: AbstractString ,
                 dataColoumns :: Array{Any,1},
                 actualData :: Array{Any , 1},
                 sDate :: AbstractString,
                 horizon:: Int)

    dateTimeObjectSDate = DateTime(sDate)
    dateIndex = getindexofdate(dataColoumns) #can be replaced by lambda function 
    
    """
    Filling the data over here
    """

    dataToReturn = Array{Any , 1}()
    if(dateIndex == -1)
        return -1
    end
    length = length(dataColoumns)
    count = 0
    while count <= horizon
        i = 1
        for i = 1 : length
            presentDateObject = DateTime(actualData[i][dateIndex])
            if(presentDateObject <= dateTimeObjectSDate)
                append!(dataToReturn , actualData[i])
                count = count + 1
            end
        end
        if(count <= horizon)
            yearTobeFeatched = Int(Dates.Year(dateTimeObjectSDate)) - 1 

            coloumns , actualData = getdataarray(securityID , Int(Dates.Year(yearTobeFeatched)))
            dateIndex = getindexofdate(coloumns)
            length = length(actualData) 
        end 
    end
    return dataToReturn
end
