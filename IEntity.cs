using System;
using System.Collections.Generic;
using System.Text;

namespace Persistence.SqlServer
{
    public interface IEntity
    {
        Guid Id { get; set; }
    }
}
